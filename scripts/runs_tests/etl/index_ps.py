from biofilter import Biofilter

bf = Biofilter("postgresql+psycopg2://bioadmin:bioadmin@localhost/biofilter_dev")
bf.etl.index()  # all indexs

# other templates
# bf.etl.index(groups="genes")
# bf.etl.index(groups=["genes", "variants"])
# bf.etl.index(drop_only=True)
# bf.etl.index(groups="variants", drop_only=True)
# bf.etl.index(drop_first=False)
# bf.etl.index(set_write_mode=False)
# bf.etl.index(set_read_mode=False)

# groups: Optional[Union[str, Iterable[str]]] = None,
# drop_only: bool = False,
# drop_first: bool = True,
# set_write_mode: bool = True,
# set_read_mode: bool = True,

# biofilter etl rebuild --help
# biofilter etl rebuild --group genes --group variant
# biofilter etl rebuild --drop-only
# biofilter etl rebuild --no-drop-first
