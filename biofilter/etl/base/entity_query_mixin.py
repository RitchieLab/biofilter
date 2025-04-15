from biofilter.db.models.entity_models import Entity, EntityName
from sqlalchemy.exc import IntegrityError


class EntityQueryMixin:

    def get_or_create_entity(
        self,
        name: str,
        group_id: int,
        # category_id: int = None,
        data_source_id: int = 0,
    ):  # noqa: E501
        """
        Finds or creates a master Entity based on the name provided.
        Returns the entity_id and a boolean indicating if it was created.
        """

        # name_clean = name.strip().upper()
        clean_gene_name = name

        # Before add, check if already exists
        # NOTE: With this approach, we can switch to a
        #       different Gene Master (HUGO, ENSEMBL, etc)
        existing = (
            self.session.query(EntityName)
            .filter_by(name=clean_gene_name)
            .first()  # noqa: E501
        )  # noqa: E501

        if existing:
            return existing.entity_id, False

        # IF NOT: create a new Entity
        new_entity = Entity(
            group_id=group_id,
            # category_id=category_id,
            # created_at=datetime.datetime.now(datetime.timezone.utc),
            # updated_at=datetime.datetime.now(datetime.timezone.utc),
        )
        self.session.add(new_entity)
        self.session.flush()

        # Add this Gene Name in the EntityName table as Primary Name
        new_name = EntityName(
            entity_id=new_entity.id,
            name=clean_gene_name,
            datasource_id=data_source_id,
            is_primary=True,
            # created_at=datetime.datetime.now(datetime.timezone.utc),
        )
        self.session.add(new_name)

        # ACID POINT to Genes Entity
        try:
            self.session.commit()
            msg = f"Entity '{clean_gene_name}' created with ID {new_entity.id}"
            self.logger.log(msg, "INFO")
            return new_entity.id, True
        except IntegrityError:
            self.session.rollback()
            msg = f"Entity creation failed for: {clean_gene_name}"
            self.logger.log(msg, "WARNING")
            return None, False

    def get_or_create_entity_name(
        self, entity_id: int, alt_name: str, data_source_id: int = 0
    ):
        """
        Adds a new alias (EntityName) to an existing Entity.
        """
        # name_clean = alt_name.strip().upper()
        name_clean = alt_name.strip()

        exists = (
            self.session.query(EntityName)
            .filter_by(entity_id=entity_id, name=name_clean)
            .first()
        )
        if exists:
            return False

        alt = EntityName(
            entity_id=entity_id,
            name=name_clean,
            datasource_id=data_source_id,  # ID do DataSource padrão
            is_primary=False,
            # created_at=datetime.datetime.now(datetime.timezone.utc),
        )
        self.session.add(alt)

        # ACID POINT to Entity Name
        try:
            self.session.commit()
            msg = f"Entity Name '{name_clean}' added to Entity ID {entity_id}"
            self.logger.log(msg, "DEBUG")
            return True
        except IntegrityError:
            self.session.rollback()
            msg = f"Couldn't add alias '{name_clean}' to Entity ID {entity_id}"
            self.logger.log(msg, "WARNING")
            return False
