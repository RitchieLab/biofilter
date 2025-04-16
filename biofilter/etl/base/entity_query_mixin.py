from sqlalchemy.exc import IntegrityError
from biofilter.db.models.entity_models import Entity, EntityName


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
            datasource_id=data_source_id,
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
            datasource_id=data_source_id,
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
