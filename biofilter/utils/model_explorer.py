# helper_sql.py

import importlib
import pandas as pd
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Session


class ModelExplorer:
    def __init__(self, session: Session, model_info: dict):
        self.session = session
        self.model_info = model_info
        self.models = self._load_models()

    def _load_models(self):
        models = {}
        for name, data in self.model_info.items():
            module_path = data["path"]
            module_name, class_name = module_path.rsplit(".", 1)
            module = importlib.import_module(module_name)
            model_class = getattr(module, class_name)
            models[name] = model_class
        return models

    def list_models(self):
        print("📦 Available Models:")
        for name in sorted(self.models):
            print(f" - {name}")

    def describe_model(self, model_name):
        model = self.models.get(model_name)
        if not model:
            print(f"⚠️ Model '{model_name}' not found.")
            return
        print(f"🧬 Model: {model_name}")
        print(self.model_info[model_name].get("description", "No description.") + "\n")

        mapper = inspect(model).mapper
        for column in mapper.columns:
            print(f" - {column.key:<25} ({str(column.type)})")


    def show_schema(self):
        print("🗂️  Database Schema Overview:")
        for name, model in self.models.items():
            columns = [c.key for c in inspect(model).mapper.column_attrs]
            print(f"\n{name}:")
            for col in columns:
                print(f"   - {col}")

    def example_query(self, model_name, limit=5):
        model = self.models.get(model_name)
        if not model:
            print("⚠️ Model not found.")
            return pd.DataFrame()
        results = self.session.query(model).limit(limit).all()
        return self._to_dataframe(results)

    def search(self, model_name, column_name, value, limit=10):
        model = self.models.get(model_name)
        if not model:
            print("⚠️ Model not found.")
            return pd.DataFrame()
        col = getattr(model, column_name, None)
        if not col:
            print("⚠️ Column not found.")
            return pd.DataFrame()
        results = (
            self.session.query(model)
            .filter(col.like(f"%{value}%"))
            .limit(limit)
            .all()
        )
        return self._to_dataframe(results)

    def _to_dataframe(self, results):
        if not results:
            return pd.DataFrame()
        return pd.DataFrame([r.__dict__ for r in results]).drop(columns=["_sa_instance_state"], errors="ignore")




"""
import json
from model_explorer import ModelExplorer
from biofilter.database import SessionLocal

with open("model_info.json") as f:
    model_info = json.load(f)

session = SessionLocal()
helper = HelperSQL(session, model_info)

helper.list_models()
helper.describe_model("Gene")
helper.example_query("Gene")



"""