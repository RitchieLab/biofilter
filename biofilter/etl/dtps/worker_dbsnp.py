import os
import re
import json
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
import pandas as pd


HGVS_G_POS = re.compile(r"^(.*?):g\.([\d_]+)(.*)$")  # ex: NC_000008.11:g.19956018A>G
NUC_CHANGE = re.compile(r"^[ACGT]>[ACGT]$")


# Using in Quality Index Only
def _extract_disease_links(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    "Para futura expansao"
    out = []
    primary = record.get("primary_snapshot_data", {}) or {}
    for aa in primary.get("allele_annotations", []) or []:
        for clin in aa.get("clinical", []) or []:
            names = clin.get("disease_names") or []
            sigs = clin.get("clinical_significances") or []
            ids = clin.get("disease_ids") or []
            label = names[0] if names else None
            sig = sigs[0] if sigs else None
            trait_id = None
            if ids:
                prio = [
                    "MONDO",
                    "OMIM",
                    "MedGen",
                    "Orphanet",
                    "HPO",
                    "Office of Rare Diseases",
                ]
                for pref in prio:
                    cand = next(
                        (d for d in ids if d.get("organization") == pref), None
                    )  # noqa E501
                    if cand:
                        trait_id = f"{cand.get('organization')}:{cand.get('accession')}"  # noqa E501
                        break
                if not trait_id:
                    d0 = ids[0]
                    trait_id = (
                        f"{d0.get('organization')}:{d0.get('accession')}"  # noqa E501
                    )
            out.append(
                {
                    "trait_id": trait_id,
                    "trait_label": label,
                    "clinical_significance": sig,
                    "source": "ClinVar" if ids or names else "dbSNP",
                }
            )
    # DEBUG: Use to check consistency
    dedup = {}
    for d in out:
        key = (
            d["trait_id"],
            d["trait_label"],
            d["clinical_significance"],
            d["source"],
        )  # noqa E501
        dedup[key] = d
    return list(dedup.values())


# Using in Quality Index Only
def _extract_frequencies(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    "Para futura expansao"
    out = []
    primary = record.get("primary_snapshot_data", {}) or {}
    for aa in primary.get("allele_annotations", []) or []:
        for fq in aa.get("frequency", []) or []:
            src = fq.get("study_name")
            # dbSNP não fornece AF direto; calculamos se possível
            ac = fq.get("allele_count")
            an = fq.get("total_count")
            af = None
            try:
                if ac is not None and an:
                    af = float(ac) / float(an)
            except Exception:
                af = None
            obs = fq.get("observation") or {}
            out.append(
                {
                    "source": src,
                    "population": None,
                    "af": af,
                    "ac": ac,
                    "an": an,
                    "seq_id": obs.get("seq_id"),
                    "position": obs.get("position"),
                }
            )
    return out


def _is_chrom_accession(seq_id: str) -> bool:
    # Conservative: RefSeq chromosome only (NC_0000xx.*)
    return isinstance(seq_id, str) and seq_id.startswith("NC_")


def _parse_hgvs_g(
    hgvs: str,
) -> Tuple[Optional[int], Optional[int], Optional[str]]:  # noqa E501
    """
    Returns (start_1b, end_1b, suffix) from genomic HGVS (g.).
    Ex: 'NC_000008.11:g.19956018A>G' -> (19956018, 19956018, 'A>G')
        '...:g.123_125del'            -> (123, 125, 'del')
        '...:g.100='                  -> (100, 100, '=')
    """
    if not hgvs:
        return None, None, None
    m = HGVS_G_POS.match(hgvs)
    if not m:
        return None, None, None
    raw_pos, suffix = m.group(2), m.group(3)
    if "_" in raw_pos:
        s, e = map(int, raw_pos.split("_"))
    else:
        s = e = int(raw_pos)
    return s, e, suffix


def _allele_type_from_suffix(suffix: Optional[str]) -> str:
    if suffix is None:
        return "oth"
    if suffix == "=":
        return "ref"
    if "delins" in suffix:
        return "delins"
    if "del" in suffix:
        return "del"
    if "dup" in suffix:
        return "dup"
    if re.search(r"\[\d+\]$", suffix):
        return "rep"
    if NUC_CHANGE.match(suffix):
        return "sub"
    return "oth"


def _extract_placements(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extracts ALL placements from the primary_snapshot_data section. Keeps:
    - is_ptlp (bool)
    - seq_id, assembly_name (if any), chromosome (empty; we can map on load)
    - start/end 1-based, allele_type, alt, hgvs
    """
    out = []
    primary = record.get("primary_snapshot_data", {})
    for p in primary.get("placements_with_allele", []):
        is_ptlp = bool(p.get("is_ptlp", False))
        pan = p.get("placement_annot", {}) or {}
        seq_traits = pan.get("seq_id_traits_by_assembly") or []
        # Some placements (NG_, NM_, NP_) do not have assembly_name. We keep it empty.  # noqa E501
        assembly_name = ""
        if seq_traits and isinstance(seq_traits, list):
            t0 = seq_traits[0]
            assembly_name = t0.get("assembly_name", "") or ""

        seq_type = pan.get("seq_type", "")
        alleles = p.get("alleles", []) or []
        for al in alleles:
            hgvs = al.get("hgvs", "")
            spdi = (al.get("allele") or {}).get("spdi") or {}
            acc = spdi.get("seq_id")
            # position in SPDI is 0-based, but we derive start/end from hgvs (1-based)  # noqa E501
            start_1b, end_1b, suffix = _parse_hgvs_g(hgvs)
            if acc is None or start_1b is None:
                continue
            alt = spdi.get("inserted_sequence", "") or ""
            allele_type = _allele_type_from_suffix(suffix)
            out.append(
                {
                    "is_ptlp": is_ptlp,
                    "seq_id": acc,
                    "assembly_name": assembly_name,
                    "chromosome": "",  # we can fill in the load via your Genome Assembly table  # noqa E501
                    "start_pos": int(start_1b),
                    "end_pos": int(end_1b),
                    "allele_type": allele_type,
                    "alt": alt,
                    "hgvs": hgvs,
                    "seq_type": seq_type,  # we save to filter downstream (e.g.: only chromosome)  # noqa E501
                }
            )
    return out


def _pick_canonical_from_ptlp(
    placements: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """
    Selects the canonical from the chromosomal PTLP (seq_type == refseq_chromosome).  # noqa E501
    Adds ref/alt found in this PTLP.
    """
    ptlp_chr = [
        x
        for x in placements
        if x["is_ptlp"]
        and x.get("seq_type") == "refseq_chromosome"
        and _is_chrom_accession(x["seq_id"])
    ]
    if not ptlp_chr:
        return None
    # We use the first one (usually there is only one chromosomal PTLP)
    acc = ptlp_chr[0]["seq_id"]
    asm = ptlp_chr[0]["assembly_name"]
    chrom = ptlp_chr[0]["chromosome"]
    # typical start/end values are the same for SNP; if there is a range,
    # we use the one in the ref (or the smallest/largest)
    starts = [p["start_pos"] for p in ptlp_chr]
    ends = [p["end_pos"] for p in ptlp_chr]
    start_pos = min(starts)
    end_pos = max(ends)

    ref_alleles = sorted(
        {p["alt"] for p in ptlp_chr if p["allele_type"] == "ref" and p["alt"]}
    )
    alt_alleles = sorted(
        {
            p["alt"]
            for p in ptlp_chr
            if p["allele_type"]
            in ("sub", "ins", "del", "delins", "dup", "rep")  # noqa E501
            and p["alt"]
        }
    )

    ref = "/".join(ref_alleles) if ref_alleles else ""
    alt = "/".join(alt_alleles) if alt_alleles else ""

    return {
        "seq_id": acc,
        "assembly_name": asm,
        "chromosome": chrom,
        "start_pos": int(start_pos),
        "end_pos": int(end_pos),
        "ref": ref,
        "alt": alt,
    }


def _extract_merge_log(record: dict) -> list[str]:
    """Return list of merged rsIDs from a dbSNP record."""
    return [
        f"rs{m['merged_rsid']}"
        for m in record.get("dbsnp1_merges", []) or []
        if m.get("merged_rsid")
    ]


def _extract_gene_links(
    record: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], List[int]]:
    genes = set()
    pmids = set(record.get("citations", []) or [])
    primary = record.get("primary_snapshot_data", {}) or {}

    # 1) Genes via assembly_annotation
    for aa in primary.get("allele_annotations", []) or []:
        for ann in aa.get("assembly_annotation", []) or []:
            for g in ann.get("genes") or []:
                gid = g.get("id")
                if gid is not None:
                    genes.add(int(gid))

        # 2) PMIDs em clinical[].citations
        for clin in aa.get("clinical", []) or []:
            for c in clin.get("citations") or []:
                try:
                    pmids.add(int(c))
                except Exception:
                    pass
            # genes via clinical.gene_ids (strings sometimes)
            for gid in clin.get("gene_ids") or []:
                try:
                    genes.add(int(gid))
                except Exception:
                    pass

    return list(genes), sorted(pmids)


def _compute_quality(
    rs_id: str,
    canonical: Optional[Dict[str, Any]],
    placements: List[Dict[str, Any]],
    citations_pmids: List[int],
    gene_links: List[Dict[str, Any]],
    disease_links: List[Dict[str, Any]],
    frequencies: List[Dict[str, Any]],
) -> Dict[str, Any]:
    has_ptlp = any(
        p["is_ptlp"]
        and p.get("seq_type") == "refseq_chromosome"
        and _is_chrom_accession(p["seq_id"])
        for p in placements
    )
    n_assemblies = len(
        {(p["seq_id"]) for p in placements if _is_chrom_accession(p["seq_id"])}
    )
    n_citations = len(citations_pmids)
    n_genes = len(gene_links)
    n_traits = len(disease_links)
    has_freq = len(frequencies) > 0
    score = (
        (5.0 if has_ptlp else 0.0)
        + min(n_assemblies, 5) * 0.5
        + min(n_citations, 10) * 0.3
        + min(n_genes, 5) * 0.5
        + (1.0 if n_traits > 0 else 0.0)
        + (1.0 if has_freq else 0.0)
    )
    return {
        "has_ptlp": has_ptlp,
        "n_assemblies": int(n_assemblies),
        "n_citations": int(n_citations),
        "n_genes": int(n_genes),
        "n_traits": int(n_traits),
        "has_freq": bool(has_freq),
        "score": float(score),
    }


def worker_dbsnp(batch, batch_id, output_dir):
    print(f"[PID {os.getpid()}] Processing batch {batch_id}")
    rows = []

    for line in batch:
        try:
            rec = json.loads(line)
            rs_id = f"rs{rec['refsnp_id']}"
            variant_type = (rec.get("primary_snapshot_data") or {}).get(
                "variant_type", ""
            )
            build_id = rec.get("last_update_build_id", None)

            # Genes
            gene_ids = {
                g.get("id")
                for ann in (rec.get("primary_snapshot_data") or {}).get(
                    "allele_annotations", []
                )
                for asm in ann.get("assembly_annotation", [])
                for g in asm.get("genes", [])
                if g.get("id")
            }

            seq_id = assembly = chromosome = None  # noqa E501
            start_pos = end_pos = None
            ref = alt = None
            alt = []
            placements_list = []

            primary = rec.get("primary_snapshot_data", {})
            for p in primary.get("placements_with_allele", []):
                is_ptlp = bool(p.get("is_ptlp", False))
                pan = p.get("placement_annot", {}) or {}
                seq_traits = pan.get("seq_id_traits_by_assembly") or []
                assembly_name = (
                    seq_traits[0].get("assembly_name", "")
                    if seq_traits
                    else ""  # noqa E501
                )
                alleles = p.get("alleles", []) or []

                for al in alleles:
                    hgvs = al.get("hgvs", "")
                    spdi = (al.get("allele") or {}).get("spdi") or {}
                    acc = spdi.get("seq_id")
                    start_1b, end_1b, suffix = _parse_hgvs_g(hgvs)
                    if not acc or not start_1b:
                        continue
                    alt_seq = spdi.get("inserted_sequence", "") or ""
                    allele_type = _allele_type_from_suffix(suffix)

                    if is_ptlp:
                        seq_id = acc
                        assembly = assembly_name
                        # chromosome = _extract_chromosome(acc)
                        start_pos = start_1b
                        end_pos = end_1b
                        if allele_type == "ref":
                            ref = alt_seq
                        elif allele_type == "sub":
                            alt.append(alt_seq)
                    else:
                        # placements_list.append(
                        #     [acc, assembly_name, start_1b, end_1b, ref, alt_seq]  # noqa E501
                        # )
                        placements_list.append(
                            {
                                "seq_id": acc,
                                "assembly": assembly_name,
                                "start_pos": int(start_1b),
                                "end_pos": int(end_1b),
                                "ref": ref,
                                "alt": alt_seq,
                            }
                        )

            # Merge log
            merge_log = [
                f"rs{m['merged_rsid']}"
                for m in rec.get("dbsnp1_merges", []) or []
                if m.get("merged_rsid")
            ]

            placements = _extract_placements(rec)
            canonical = _pick_canonical_from_ptlp(placements)
            merge_log = _extract_merge_log(rec)
            gene_links, pmids = _extract_gene_links(rec)
            disease_links = _extract_disease_links(rec)
            frequencies = _extract_frequencies(rec)

            quality = _compute_quality(
                rs_id,
                canonical,
                placements,
                pmids,
                gene_links,
                disease_links,
                frequencies,
            )

            rows.append(
                {
                    "rs_id": rs_id,
                    "variant_type": variant_type,
                    "build_id": build_id,
                    "seq_id": seq_id,
                    "assembly": assembly,
                    # "chromosome": chromosome,
                    "start_pos": start_pos,
                    "end_pos": end_pos,
                    "ref": ref,
                    "alt": alt,
                    "placements": placements_list,
                    "merge_log": merge_log,
                    "gene_links": list(gene_ids),
                    "quality": quality["score"],
                }
            )

        except Exception as e:
            print(f"[PID {os.getpid()}] ⚠️ Error in batch {batch_id}: {e}")
            continue

    if rows:
        df = pd.DataFrame(rows)

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        out_path = output_dir / f"processed_part_{batch_id}.parquet"

        def as_list_of_dicts(x):
            if isinstance(x, np.ndarray):
                x = x.tolist()
            return (
                list(x)
                if isinstance(x, (list, tuple))
                else ([] if x is None else [x])  # noqa E501
            )

        df["placements"] = df["placements"].apply(as_list_of_dicts)

        def fix_list_of_str(x):
            if isinstance(x, np.ndarray):
                return x.tolist()
            if (
                isinstance(x, list)
                and len(x) == 1
                and isinstance(x[0], np.ndarray)  # noqa E501
            ):  # noqa E501
                return x[0].tolist()
            return x

        df["merge_log"] = df["merge_log"].apply(fix_list_of_str)
        df["gene_links"] = df["gene_links"].apply(fix_list_of_str)

        df.to_parquet(out_path, index=False)

        # DEBUG use only
        # out_path_csv = output_dir / f"processed_part_{batch_id}.csv"
        # df.to_csv(out_path_csv)

        print(
            f"[PID {os.getpid()}] ✅ Finished batch {batch_id}, saved {len(df)} rows → {out_path}"  # noqa E501
        )
    else:
        print(f"[PID {os.getpid()}] ⚠️ No rows produced for batch {batch_id}")
