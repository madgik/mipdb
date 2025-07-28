from collections import defaultdict
import re

from sqlalchemy import text
from sqlalchemy_monetdb.dialect import MonetDialect
from sqlalchemy_monetdb.dialect import quote
from sqlalchemy_monetdb.monetdb_types import MONETDB_TYPE_MAP


class PatchedMonetDialect(MonetDialect):
    def _get_columns(
        self,
        connection: "Connection",
        filter_names=[],
        schema=None,
        temp=0,
        tabletypes=[0, 1],
        **kw,
    ):
        ischema = schema
        columns = defaultdict(list)
        if len(tabletypes) == 0:
            return columns.items()
        if temp == 1:
            if not schema:
                ischema = "tmp"
        for table_name in filter_names:
            q = ""
            args = {}
            if ischema:
                q = """SELECT c.name, c."type", c.type_digits digits, c.type_scale scale, c."null", c."default" cdefault, c.number
                FROM sys.tables t, sys.schemas s, sys.columns c
                        WHERE c.table_id = t.id
                        AND t.name = :table
                        AND t.schema_id = s.id
                        AND t.temporary = :_temp
                        AND t.type in ( %s )
                        AND s.name = :schema
                ORDER BY c.number
                """ % (
                    ", ".join(str(tt) for tt in tabletypes)
                )
                args = {"table": table_name, "schema": ischema, "_temp": temp}
            else:
                q = """SELECT c.name, c."type", c.type_digits digits, c.type_scale scale, c."null", c."default" cdefault, c.number
                    FROM sys.tables t, sys.schemas s, sys.columns c
                            WHERE c.table_id = t.id
                            AND t.name = :table
                            AND t.schema_id = s.id
                            AND t.temporary = :_temp
                            AND t.type in ( %s )
                            AND s.name = CURRENT_SCHEMA
                    ORDER BY c.number
                    """ % (
                    ", ".join(str(tt) for tt in tabletypes)
                )
                args = {"table": table_name, "_temp": temp}
            c = connection.execute(text(q), args)

            if c.rowcount == 0:
                continue
            sequences = []
            result = columns[(schema, table_name)]
            for row in c:
                args = ()
                kwargs = {}
                name = row.name
                if row.type in ("char", "varchar"):
                    args = (row.digits,)
                elif row.type == "decimal":
                    args = (row.digits, row.scale)
                elif row.type == "timestamptz":
                    kwargs = {"timezone": True}
                col_type = MONETDB_TYPE_MAP.get(row.type, None)
                if col_type is None:
                    raise TypeError(
                        "Can't resolve type {0} (column '{1}')".format(col_type, name)
                    )
                col_type = col_type(*args, **kwargs)

                # monetdb translates an AUTO INCREMENT into a sequence
                autoincrement = False
                cdefault = row.cdefault
                if cdefault is not None:
                    r = r"""next value for \"(\w*)\"\.\"(\w*)"$"""
                    match = re.search(r, cdefault)
                    if match is not None:
                        seq_schema = match.group(1)
                        seq = match.group(2)
                        autoincrement = True
                        cdefault = None
                        sequences.append((name, seq))

                column = {
                    "name": name,
                    "type": col_type,
                    "default": cdefault,
                    "autoincrement": autoincrement,
                    "nullable": row.null,
                }

                result.append(column)

            if sequences:
                for (name, seq) in sequences:
                    seq_info = self._get_sequence(connection, seq, schema=seq_schema);
                    if seq_info:
                        for c in result:
                            if c["name"] == name:
                                c["identity"] = {"start": seq_info[0][1],
                                                 "increment": seq_info[0][2] }

        return columns.items()

    def _get_foreign_keys(
        self,
        connection: "Connection",
        schema=None,
        filter_names=[],
        temp=0,
        tabletypes=[0, 1],
        **kw,
    ):
        """Return information about foreign_keys in `table_name`.

        Given a string `table_name`, and an optional string `schema`, return
        foreign key information as a list of dicts with these keys:

        constrained_columns
          a list of column names that make up the foreign key

        referred_schema
          the name of the referred schema

        referred_table
          the name of the referred table

        referred_columns
          a list of column names in the referred table that correspond to
          constrained_columns

        name
          optional name of the foreign key constraint.

        **kw
          other options passed to the dialect's get_foreign_keys() method.

        """

        ischema = schema
        fkeys = defaultdict(list)
        if len(tabletypes) == 0 or len(filter_names) == 0:
            return fkeys.items()
        if temp == 1:
            if not schema:
                ischema = "tmp"

        q = """
        WITH action_type (id, act) AS (VALUES (0, 'NO ACTION'), (1, 'CASCADE'), (2, 'RESTRICT'), (3, 'SET NULL'), (4, 'SET DEFAULT'))
        select fk_s, fk_t, fk_c, o, fk, pk_s, pk_t, pk_c, on_update, on_delete from
        (select fs.name fk_s, fkt.name fk_t, fkt.id id
         from sys.tables as fkt, sys.schemas as fs
        where fkt.temporary = :_temp AND fkt.type in (%s) AND fkt.schema_id = fs.id AND fs.name = %s AND fkt.name in (%s)) f
        LEFT OUTER JOIN
        (select fkkc.name fk_c, fkkc.nr o, fkk.name fk, fkk.table_id fktid, ps.name pk_s, pkt.name pk_t, pkkc.name pk_c, ou.act on_update, od.act on_delete
        from sys.objects fkkc, sys.keys fkk, sys.tables pkt, sys.objects pkkc, sys.keys pkk, sys.schemas ps, action_type ou, action_type od
                WHERE pkt.id = pkk.table_id
                AND fkk.id = fkkc.id
                AND pkk.id = pkkc.id
                AND fkk.rkey = pkk.id
                AND fkkc.nr = pkkc.nr
                AND pkt.schema_id = ps.id
                AND (fkk."action" & 255)         = od.id
                AND ((fkk."action" >> 8) & 255)  = ou.id ) as fk
        on f.id = fk.fktid
ORDER BY fk_t, fk, o
        """ % (
            (", ".join(str(tt) for tt in tabletypes)),
            quote(ischema) if ischema else "CURRENT_SCHEMA",
            ", ".join(quote(table_name) for table_name in filter_names),
        )
        args = {"_temp": temp}
        c = connection.execute(text(q), args)

        key_data = None
        constrained_columns = []
        referred_columns = []
        last_name = None
        table_name = None
        ondelete = None
        onupdate = None
        cnt = 0

        for row in c:
            if cnt and (last_name != row.fk or row.fk is None):
                if key_data:
                    key_data["constrained_columns"] = constrained_columns
                    key_data["referred_columns"] = referred_columns
                    key_data["options"] = {
                        k: v
                        for k, v in [
                            ("onupdate", onupdate),
                            ("ondelete", ondelete),
                            # ("initially", False),
                            # ("deferrable", False),
                            # ("match", "full"),
                        ]
                        if v is not None and v != "NO ACTION"
                    }
                    results.append(key_data)
                constrained_columns = []
                referred_columns = []
                ondelete = None
                onupdate = None
                key_data = None
                if table_name != row.fk_t:
                    table_name = row.fk_t
                    results = fkeys[(schema, table_name)]

            if table_name is None or last_name != row.fk:
                if row.fk:
                    key_data = {
                        "name": row.fk,
                        "referred_schema": row.pk_s if schema else None,
                        "referred_table": row.pk_t,
                    }
                    ondelete = row.on_delete
                    onupdate = row.on_update
                table_name = row.fk_t
                results = fkeys[(schema, table_name)]

            last_name = row.fk
            cnt += 1
            if row.fk:
                constrained_columns.append(row.fk_c)
                referred_columns.append(row.pk_c)

        if key_data:
            key_data["constrained_columns"] = constrained_columns
            key_data["referred_columns"] = referred_columns
            key_data["options"] = {
                k: v
                for k, v in [
                    ("onupdate", onupdate),
                    ("ondelete", ondelete),
                    # ("initially", False),
                    # ("deferrable", False),
                    # ("match", "full"),
                ]
                if v is not None and v != "NO ACTION"
            }
            results.append(key_data)

        data = fkeys.items()
        return data

    def _get_indexes(
        self,
        connection: "Connection",
        filter_names=[],
        schema=None,
        temp=0,
        tabletypes=[0, 1],
        **kw,
    ):
        """
        ReflectedIndex list
            column_names: List[str | None],
            column_sorting: NotRequired[Dict[str, Tuple[str]]],
            dialect_options: NotRequired[Dict[str, Any]],
            duplicates_constraint: NotRequired[str | None],
            expressions: NotRequired[List[str]],
            include_columns: NotRequired[List[str]],
            name: str | None,
            unique: bool
        """

        ischema = schema
        idxs = defaultdict(list)
        if len(tabletypes) == 0 or len(filter_names) == 0:
            return idxs.items()
        if temp == 1:
            if not schema:
                ischema = "tmp"

        q = """ WITH it (id, idx) AS (VALUES (0, 'INDEX'), (1, 'JOININDEX'), (2, '2'), (3, '3'), (4, 'IMPRINTS INDEX'), (5, 'ORDERED INDEX')), --UNIQUE INDEX wraps to INDEX.
        tbls (id, tbl, sch) AS (
                SELECT t.id, t.name, s.name
                FROM sys.schemas s, sys.tables t
                where
                    s.id = t.schema_id
                    AND t.system = FALSE
                    AND t.type in ( %s )
                    AND s.name = %s
                    AND t.name in ( %s )
                    AND t.temporary = :_temp),
        indices( ind, col, tpe, knr, table_id ) AS (
        SELECT  i.name ind, kc.name col, it.idx tpe, kc.nr knr, i.table_id table_id
        FROM    sys.idxs AS i LEFT JOIN sys.keys AS k ON i.name = k.name, sys.objects kc, tbls t, it
        WHERE   i.id = kc.id
                AND k.type IS NULL
                AND i.type = it.id
        UNION
        SELECT  k.name ind, kc.name col, 'UNIQUE' tpe, kc.nr knr, k.table_id table_id
        FROM    sys.keys k, sys.objects kc, tbls t
        WHERE   k.id = kc.id
                AND k.type = 1
        )
        select ind, sch, tbl, col, tpe, knr
        from tbls t LEFT OUTER JOIN indices i
        on i.table_id = t.id
        ORDER BY tbl, ind, tpe, knr
        """ % (
            (", ".join(str(tt) for tt in tabletypes)),
            quote(ischema) if ischema else "CURRENT_SCHEMA",
            ", ".join(quote(table_name) for table_name in filter_names),
        )
        args = {"_temp": temp}
        c = connection.execute(text(q), args)

        index_data = None
        column_names = []
        last_name = None
        table_name = None
        cnt = 0

        for row in c:
            if cnt and (last_name != row.ind or row.ind is None):
                if index_data:
                    index_data["column_names"] = column_names
                    results.append(index_data)
                index_data = None
                column_names = []
                if table_name != row.tbl:
                    table_name = row.tbl
                    results = idxs[(schema, table_name)]

            if table_name is None or last_name != row.ind:
                if row.ind:
                    index_data = {
                        "name": row.ind,
                        "unique": True if row.tpe == "UNIQUE" else False,
                        "include_columns": [],
                        "dialect_options": {},
                    }
                    if row.tpe == "UNIQUE":
                        index_data["duplicates_constraint"] = row.ind
                table_name = row.tbl
                results = idxs[(schema, table_name)]

            last_name = row.ind
            cnt += 1
            if row.ind:
                column_names.append(row.col)

        if index_data:
            index_data["column_names"] = column_names
            results.append(index_data)

        data = idxs.items()
        return data

    def get_unique_constraints(
        self, connection: "Connection", table_name, schema=None, **kw
    ):
        """Return information about unique constraints in `table_name`.

        Given a string `table_name` and an optional string `schema`, return
        unique constraint information as a list of dicts with these keys:

        name
          the unique constraint's name

        column_names
          list of column names in order

        **kw
          other options passed to the dialect's get_unique_constraints() method.

        .. versionadded:: 0.9.0

        """

        q = """
        SELECT o.name "col", k.name "name"
                 FROM sys.keys k,
                         sys.objects o,
                         sys.tables t,
                         sys.schemas s
                 WHERE k.id = o.id
                         AND k.table_id = t.id
                         AND t.schema_id = s.id
                         AND k.type = 1
                         AND t.id = :table_id
                order by "name", o.nr
        """
        args = {"table_id": self._table_id(connection, table_name, schema)}
        c = connection.execute(text(q), args)
        table = c.fetchall()

        col_dict = defaultdict(list)
        for col, name in table:
            col_dict[name].append(col)

        res = [{"column_names": c, "name": n} for n, c in col_dict.items()]
        return res

    def get_check_constraints(self, connection, table_name, schema, **kw):
        """Return information about check constraints in `table_name`.

        Given a string `table_name` and an optional string `schema`, return
        check constraint information as a list of dicts with these keys:

        name
          name of check constraint

        sqltext
            the check constraint’s SQL expression

        **kw
          other options passed to the dialect's get_check_constraints() method.

        .. versionadded:: 2.0.0

        """
        if not self.server_version_info >= (11, 51, 3):
            raise NotImplementedError(
                "CHECK constraint are supported only by "
                "MonetDB server 11.51.3 or greater"
            )

        q = """
        SELECT k.name "name", sys.check_constraint(:schema, k.name) sqltext
                 FROM
                    sys.tables t,
                    sys.keys k
                 WHERE
                    k.table_id = t.id AND
                    t.id = :table_id AND
                    k.type = 4
                order by "name"
        """

        if schema is None:
            schema = connection.execute(text("SELECT current_schema")).scalar()

        args = {"table_id": self._table_id(connection, table_name, schema), "schema": schema}
        c = connection.execute(text(q), args)
        table = c.fetchall()

        res = [{"name": name, "sqltext": sqltext} for name, sqltext in table]
        return res
