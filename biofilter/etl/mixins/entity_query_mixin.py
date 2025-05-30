from sqlalchemy.exc import IntegrityError
from biofilter.db.models.entity_models import Entity, EntityName, EntityRelationship


class EntityQueryMixin:
    def get_or_create_entity(
        self,
        name: str,
        group_id: int,
        data_source_id: int = 0,
    ):
        """
        Finds or creates a master Entity based on the primary name.

        Returns:
            Tuple (entity_id, created): The ID of the Entity and a boolean
            indicating whether a new entity was created.
        """
        clean_name = name.strip()

        existing = (
            self.session.query(EntityName).filter_by(name=clean_name).first()
        )  # noqa E501
        if existing:
            return existing.entity_id, False

        # Create Entity and its primary EntityName
        new_entity = Entity(group_id=group_id)
        self.session.add(new_entity)
        self.session.flush()

        primary_name = EntityName(
            entity_id=new_entity.id,
            name=clean_name,
            data_source_id=data_source_id,
            is_primary=True,
        )
        self.session.add(primary_name)

        try:
            self.session.commit()
            msg = f"✅ Entity '{clean_name}' created with ID {new_entity.id}"
            self.logger.log(msg, "INFO")
            return new_entity.id, True
        except IntegrityError:
            self.session.rollback()
            msg = f"⚠️ Entity creation failed for: {clean_name}"
            self.logger.log(msg, "WARNING")
            return None, False

    def get_or_create_entity_name(
        self,
        entity_id: int,
        alt_name: str,
        data_source_id: int = 0,
    ):
        """
        Adds an alias (EntityName) to an existing Entity.

        Returns:
            True if added successfully, False if already exists or failed.
        """
        clean_name = alt_name.strip()

        exists = (
            self.session.query(EntityName)
            .filter_by(entity_id=entity_id, name=clean_name)
            .first()
        )
        if exists:
            return False

        alias = EntityName(
            entity_id=entity_id,
            name=clean_name,
            data_source_id=data_source_id,
            is_primary=False,
        )
        self.session.add(alias)

        try:
            self.session.commit()
            msg = f"✅ Alias '{clean_name}' added to Entity {entity_id}"
            self.logger.log(msg, "DEBUG")
            return True
        except IntegrityError:
            self.session.rollback()
            msg = f"⚠️ Couldn't add alias '{clean_name}' to Entity {entity_id}"
            self.logger.log(msg, "WARNING")
            return False

    def get_or_create_entity_relationship(
        self,
        entity_1_id: int,
        entity_2_id: int,
        relationship_type_id: int,
        data_source_id: int,
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
            relationship_type_id=relationship_type_id,
            data_source_id=data_source_id,
        )

        self.session.add(rel)

        return True

        # Nao realizando o commit aqui, pois o commit deve ser feito no final do ETL
        # try:
        #     self.session.commit()
        #     msg = f"✅ Added relationship {entity_1_id} -> {entity_2_id} (type {relationship_type_id})"
        #     self.logger.log(msg, "DEBUG")
        #     return True
        # except IntegrityError:
        #     self.session.rollback()
        #     msg = f"⚠️ IntegrityError adding relationship {entity_1_id} -> {entity_2_id} (type {relationship_type_id})"
        #     self.logger.log(msg, "WARNING")
        #     return False
