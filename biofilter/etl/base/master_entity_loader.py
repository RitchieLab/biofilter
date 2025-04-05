from biofilter.db.models.entity_models import Entity, EntityName
from sqlalchemy.exc import IntegrityError
import datetime


class MasterEntityLoader:
    def get_or_create_entity(self, name: str, group_id: int, category_id: int):
        """
        Finds or creates a master Entity based on the name provided.
        Returns the entity_id and a boolean indicating if it was created.
        """
        name_clean = name.strip().upper()

        # Verifica se já existe um EntityName com esse nome
        existing = (
            self.session.query(EntityName)
            .filter_by(name=name_clean)
            .first()
        )

        if existing:
            return existing.entity_id, False

        # Criar novo Entity
        new_entity = Entity(
            group_id=group_id,
            category_id=category_id,
            created_at=datetime.datetime.now(datetime.timezone.utc),
            updated_at=datetime.datetime.now(datetime.timezone.utc),
        )
        self.session.add(new_entity)
        self.session.flush()  # para obter o ID do novo Entity

        # Criar o primeiro nome
        new_name = EntityName(
            entity_id=new_entity.id,
            name=name_clean,
            source="ETL",
            created_at=datetime.datetime.now(datetime.timezone.utc),
        )
        self.session.add(new_name)

        try:
            self.session.commit()
            self.logger.log(f"✅ Entity created: {name_clean} -> ID {new_entity.id}", "INFO")
            return new_entity.id, True
        except IntegrityError:
            self.session.rollback()
            self.logger.log(f"⚠️ Entity creation failed for: {name_clean}", "WARNING")
            return None, False

    def add_entity_name(self, entity_id: int, alt_name: str, source: str = "ETL"):
        """
        Adds a new alias (EntityName) to an existing Entity.
        """
        name_clean = alt_name.strip().upper()

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
            source=source,
            created_at=datetime.datetime.now(datetime.timezone.utc),
        )
        self.session.add(alt)
        try:
            self.session.commit()
            self.logger.log(f"➕ Added alias '{name_clean}' to Entity ID {entity_id}", "DEBUG")
            return True
        except IntegrityError:
            self.session.rollback()
            self.logger.log(f"⚠️ Could not add alias '{name_clean}' to Entity ID {entity_id}", "WARNING")
            return False
