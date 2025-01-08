# tests/conftest.py
import sys
import os

# add the biofilter directory to sys.path to ensure the tests find it
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../biofilter_modules"))
)
