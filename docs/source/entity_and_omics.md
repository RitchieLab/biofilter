# The Entity Model

What Biofilter means by a gene, and why you can call it whatever you like.

## One concept, many names

Every biological object Biofilter knows is an **entity** — a single row that
stands for one concept. Around it sit the names it goes by and the things it
connects to:

| | |
|---|---|
| `entities` | The concept itself. One row per gene, protein, pathway, disease, GO term, chemical. |
| `entity_aliases` | Every name, symbol, synonym and external code that points at it. |
| `entity_relationships` | A typed, directed link between two entities. |
| `entity_relationship_types` | What a link means: `interacts_with`, `in_pathway`, `encodes`, `is_a`. |
| `entity_groups` | Which domain an entity belongs to. |

The practical effect is the one that matters to you: **you do not have to
know which name a source used.** `TP53`, `ENSG00000141510`, `HGNC:11998` and
`7157` all resolve to the same entity, so a gene list assembled from three
different papers works without being harmonized first.

In one full core build, that looked like this:

| | |
|---|---:|
| Entities | 203,393 |
| Aliases pointing at them | 912,316 |
| Relationships between them | 4,210,591 |

Roughly 4.5 names per concept. That ratio is the integration work the
platform is doing on your behalf.

Where the names come from, by source:

| Source | Aliases |
|---|---:|
| HGNC | 250,301 |
| MONDO | 155,752 |
| UniProt | 111,490 |
| ENTREZ | 70,220 |
| ENSEMBL | 45,863 |
| GO | 41,378 |
| NCBI | 26,224 |
| UCSC | 24,337 |
| MEDGEN | 21,660 |

One practical caution: `xref_source` is recorded as each source spelled it,
and the spellings are not normalized — `UniProt` and `Uniprot` both appear,
as separate values. Match case-insensitively if you filter on it.

## Following the links

Relationships are what make a cross-domain question answerable in one query:
gene → pathway → disease is a traversal, not a join you have to hand-write
per domain. The types actually present in a full build:

| Type | Links |
|---|---:|
| `interacts_with` | 3,950,396 |
| `in_pathway` | 180,646 |
| `is_a` | 46,580 |
| `encodes` | 20,255 |
| `part_of` | 6,511 |
| `Disease_has_disruption` | 6,203 |

`expand_entity_relationship` returns these rows directly;
`expand_entity_neighborhood` summarises them as degree per entity; and
`pair_variants` uses them to decide whether two genes share enough biology
to be worth reporting.

## Domain detail

The entity layer carries identity and connection. The specifics live in
master tables beside it — `gene_masters`, `protein_masters`,
`pathway_masters`, `disease_masters`, `go_masters`, `chemical_masters` —
each linked back to its entity. That is where you find a gene's locus type,
a protein's Pfam domains, a disease's cross-references.

The `annotate_*` reports are the read interface over this layer: give them
names, get back the master detail plus relationship counts.

## Variants are deliberately outside this

This is the exception worth knowing, because the rest of the model does not
predict it.

**Variants are not entities.** They carry no `entity_id`, appear in no
relationship, and the `Variants` entity group is empty in every bundle. A
variant is identified by `chromosome:position:ref:alt` and nothing else.

They reach genes through the symbol and HGNC id that VEP emitted — a string
match, not a link through the entity graph. So when `expand_gene_to_variant`
asks you to choose between `mapping=position` and `mapping=annotation`, that
is the reason: there is no stored edge saying a variant belongs to a gene,
so you have to say which question you mean.

This is a design decision, not a gap. Variants outnumber every other domain
by three orders of magnitude, and keeping them out of the entity graph is
what lets them be built, stored and queried independently of it.

## Which domains a bundle actually holds

Fourteen entity groups are defined. Far fewer are populated, because a group
is only filled by a source that was built. From the same build:

| Group | Entities |
|---|---:|
| Genes | 72,660 |
| Proteins | 53,296 |
| Gene Ontology | 38,092 |
| Diseases | 36,090 |
| Pathways | 3,255 |
| Chemicals | 0 — ChEBI was not included in this build |
| Variants | 0 — by design, see above |

Epigenomics, Transcriptomics, Metabolomics, Clinical Trials, Microbiome,
Phenotypes and Cell Types are defined and empty. They mark room the model
leaves for domains that have no source behind them yet, and no report will
return anything for them.

**Check before you assume.** The bundle you were given may not carry the
domain your question needs:

```bash
biofilter --bundle <path> report run --report-name platform_data_statistics
```

That reports entity counts by domain for the bundle in front of you, which
is the only authority on what it can answer. The numbers on this page come
from one build and are there to show the shape, not to be quoted.

## See also

- [Report Catalog](report_catalog.md) — which report reads which layer
- [Database Schema](technical/schema.md) — every table and column
