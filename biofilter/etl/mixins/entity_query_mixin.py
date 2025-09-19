from sqlalchemy.exc import IntegrityError
from biofilter.db.models.model_entities import (
    Entity,
    EntityAlias,
    EntityRelationship,  # noqa: E501
)
from biofilter.utils.utilities import string_normalization, as_list


class EntityQueryMixin:

    def get_or_create_entity(
        self,
        name: str,
        group_id: int,
        data_source_id: int = 0,
        package_id: int = None,
        alias_type: str = None,
        xref_source: str = None,
        alias_norm: str = None,
        is_active: bool = True,
        force_create: bool = False,
    ):
        """
        Returns the ID of an existing entity or creates a new one with its
        primary alias.

        Args:
            force_create (bool): If True, skip the SELECT and directly create
            the Entity + Alias.
            name (str): Primary name (ex: rsID, Gene Symbol, etc).
            group_id (int): FK to EntityGroup.
            data_source_id (int): FK to DataSource.
            package_id (int): Optional FK to Package (for traceability).
            alias_type (str): Label for alias (ex: "rsID", "Symbol").
            xref_source (str): Cross-reference source (ex: "dbSNP", "HGNC").
            alias_norm (str): Optional normalized version of the alias.
            is_active (bool): Whether this entity is active (for filters).

        Returns:
            Tuple[int, bool]: (entity_id, is_new)
        """

        try:
            if not name:
                raise ValueError("Entity name must be provided")

            clean_name = name.strip()

            # IF not forcing, check if exists by Alias Name
            if not force_create:
                query = self.session.query(EntityAlias).filter_by(
                    alias_value=clean_name,
                    alias_type=alias_type,
                    xref_source=xref_source,
                    is_primary=True,
                )
                # TODO: Try to check conflicts (ex. FACL1)

                existing = query.first()
                if existing:
                    return existing.entity_id, False

            # Create a new Entity
            new_entity = Entity(
                group_id=group_id,
                is_active=is_active,
                data_source_id=data_source_id,
                etl_package_id=package_id,
            )
            self.session.add(new_entity)
            self.session.flush()

            # Create the primary EntityAlias
            primary_alias = EntityAlias(
                entity_id=new_entity.id,
                group_id=group_id,
                alias_value=clean_name,
                alias_type=alias_type,
                xref_source=xref_source,
                alias_norm=alias_norm,
                is_primary=True,
                is_active=is_active,
                locale="en",
                data_source_id=data_source_id,
                etl_package_id=package_id,
            )
            self.session.add(primary_alias)

            self.session.commit()
            msg = f"✅ Entity '{clean_name}' created with ID {new_entity.id}"
            # self.logger.log(msg, "DEBUG")
            return new_entity.id, True

        except Exception as e:
            self.session.rollback()
            msg = f"⚠️ Insert Entity failed for: {clean_name} with error: {e}"
            self.logger.log(msg, "WARNING")
            return None, False

    def get_or_create_entity_name(
        self,
        group_id: int,
        entity_id: int,
        aliases: list[dict],
        is_active: bool = True,
        data_source_id: int = 0,
        package_id: int = None,
        force_create: bool = False,
    ) -> int:

        existing_keys = {}

        # IF not forcing, check if exists by Alias Name
        if not force_create:
            existing = (
                self.session.query(EntityAlias)
                .filter_by(entity_id=entity_id)
                .all()  # noqa E501
            )  # noqa E501
            existing_keys = {
                (e.alias_value, e.alias_type, e.xref_source) for e in existing
            }  # noqa E501

        count_added = 0

        # Drop duplicated alias values
        key = "alias_norm"  # or "alias_value"
        seen = set()
        unique, dropped = [], []

        for a in aliases:
            val = (a.get(key)).strip()
            if not val:
                unique.append(a)
                continue

            if val in seen:
                dropped.append(a)
            else:
                seen.add(val)
                unique.append(a)

        for alias in unique:
            key = (
                alias["alias_value"].strip(),
                alias["alias_type"],
                alias["xref_source"],
            )  # noqa E501
            if key in existing_keys:
                continue

            new_alias = EntityAlias(
                entity_id=entity_id,
                group_id=group_id,
                alias_value=key[0],
                alias_type=key[1],
                xref_source=key[2],
                alias_norm=alias.get("alias_norm"),
                locale=alias.get("locale", "en"),
                is_primary=False,
                is_active=is_active,
                data_source_id=data_source_id,
                etl_package_id=package_id,
            )
            self.session.add(new_alias)
            count_added += 1

        try:
            self.session.commit()
            self.logger.log(
                f"✅  {count_added} aliases added to Entity {entity_id}",
                "DEBUG",  # noqa E501
            )
            return count_added
        except IntegrityError as e:
            self.session.rollback()
            self.logger.log(
                f"⚠️  Rollback while adding aliases to Entity {entity_id}",
                "ERROR",  # noqa E501
            )
            self.logger.log(f"  --> Error: {e}", "DEBUG")
            return 0

    def get_or_create_entity_relationship(
        self,
        entity_1_id: int,
        entity_2_id: int,
        entity_1_group_id: None,
        entity_2_group_id: None,
        relationship_type_id: int,
        data_source_id: int,
        package_id: int = None,
    ):
        """
        Get or create an EntityRelationship.
        Ensures that no duplicates are created.
        """

        # Check if relationship already exists
        rel = (
            self.session.query(EntityRelationship)
            .filter_by(
                entity_1_id=entity_1_id,
                entity_2_id=entity_2_id,
                relationship_type_id=relationship_type_id,
                data_source_id=data_source_id,
            )
            .first()
        )

        if rel:
            return False  # Already exists

        # Create new relationship
        rel = EntityRelationship(
            entity_1_id=entity_1_id,
            entity_2_id=entity_2_id,
            entity_1_group_id=entity_1_group_id,
            entity_2_group_id=entity_2_group_id,
            relationship_type_id=relationship_type_id,
            data_source_id=data_source_id,
            etl_package_id=package_id,
        )

        self.session.add(rel)

        return True

    # --- THESE METHODS WILL CREATE A DICT OF ALIASES ---

    # def _coerce_hgnc(self, v: str) -> str:
    #     v = v.strip()
    #     return v if v.upper().startswith("HGNC:") else f"HGNC:{v}"

    def build_alias(self, row: dict) -> list[dict]:
        payloads = []

        for key, (atype, src, is_primary) in self.alias_schema.items():
            for raw in as_list(row.get(key)):
                # NOTE: If necessary, return this rule and method
                # alias_value = (
                #     self._coerce_hgnc(raw) if key == "hgnc_id" else raw
                # )  # noqa E501
                alias_value = raw

                payloads.append(
                    {
                        "alias_value": alias_value,
                        "alias_type": atype,
                        "xref_source": src,
                        "is_primary": is_primary,
                        "alias_norm": string_normalization(alias_value),
                        "locale": "en",
                    }
                )

        # Drop Duplicated was transfer to add Entity Aliases Method
        return payloads
