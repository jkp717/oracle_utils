from __future__ import annotations

import os
import re
import platform
from typing import List, Tuple, Optional, Union, IO
import dotenv

import oracledb
import sqlglot.expressions
from oracledb.exceptions import DatabaseError
from sqlglot import parse_one, exp
from sqlglot.optimizer.qualify import qualify
from sqlglot.errors import ErrorLevel


class OracleAPI:

    def __init__(self, username: str = None, password: str = None, host: str = None, port: int = 1521, service: str = None,
                 thin_mode: bool = True, client_path: str = None, env_path: str = None):

        self._pk_cache = {}
        self._env_path = env_path
        if env_path:
            dotenv.load_dotenv(self._env_path)

        if not thin_mode:
            _client = None
            if platform.system() == "Windows":
                if client_path:
                    _client = r"%s" % client_path
                else:
                    _client = r"%s" % os.environ['ORACLE_CLIENT_LIB']

            elif platform.system() == "Linux":
                if client_path:
                    os.system(f'export LD_LIBRARY_PATH="{client_path}:$LD_LIBRARY_PATH"')
                else:
                    lib_path = os.environ['ORACLE_CLIENT_LIB']
                    os.system(f'export LD_LIBRARY_PATH="{lib_path}:$LD_LIBRARY_PATH"')

            oracledb.init_oracle_client(lib_dir=_client)

        if username and password:
            self._uid = username
            self._pw = password
        else:
            self._load_env_credentials()

        self._host = host if host else self._load_env_host()
        self._service = service if service else self._load_env_service()
        self._port = port if port else self._load_env_port()

        self._connection = oracledb.connect(
            user=self._uid,
            password=self._pw,
            host=self._host,
            port=self._port,
            service_name=self._service
        )
        if not self._connection.is_healthy():
            raise DatabaseError('Connection failure')

        self._cursor = self._connection.cursor()

    def _load_env_credentials(self) -> Tuple[str, str]:
        self._uid = os.getenv('ORACLE_USERNAME')
        self._pw = os.getenv('ORACLE_PASSWORD')

        if not self._uid or not self._pw:
            raise Exception(f"""
            Unable to find Oracle credentials environment variables. Please ensure a .env file is
            present at {self._env_path} with ORACLE_USERNAME and ORACLE_PASSWORD set. Or provide 
            credentials in class instance.
            """)
        return self._uid, self._pw

    def _load_env_host(self) -> str:
        self._host = os.getenv('ORACLE_HOST')

        if not self._host:
            raise Exception(f"""
            Unable to find oracle host environment variable. Please ensure a .env file is
            present at {self._env_path} with ORACLE_HOST set. Or provide host in class instance.
            """)
        return self._host

    def _load_env_service(self) -> str:
        self._service = os.getenv('ORACLE_SERVICE')

        if not self._service:
            raise Exception(f"""
            Unable to find oracle service name environment variable. Please ensure a .env file is
            present at {self._env_path} with ORACLE_SERVICE set. Or provide service in class instance.
            """)
        return self._service

    def _load_env_port(self) -> int:
        self._port = int(os.getenv('ORACLE_PORT'))

        if not self._port:
            raise Exception(f"""
            Unable to find oracle port environment variable. Please ensure a .env file is
            present at {self._env_path} with ORACLE_PORT set. Or provide port in class instance.
            """)
        return self._port

    @property
    def cursor(self):
        if self._cursor:
            return self._cursor
        self._cursor = self._connection.cursor()
        return self._cursor

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()

    def close(self):
        self._connection.close()

    def _pk_caching(self, db_table: str, schema: Optional[str] = None) -> List:
        full_path = f"{schema}.{db_table}" if schema else db_table
        # Check Cache for PK metadata, otherwise fetch it
        if full_path not in getattr(self, '_pk_cache', {}):
            if not hasattr(self, '_pk_cache'): self._pk_cache = {}
            pk_sql = """
                  SELECT column_name FROM all_cons_columns 
                  WHERE constraint_name = (
                      SELECT constraint_name FROM all_constraints 
                      WHERE UPPER(table_name) = UPPER(:table_name) 
                      AND UPPER(owner) = NVL(:owner, (SELECT UPPER(user) FROM dual))
                      AND constraint_type = 'P'
                  ) ORDER BY position
              """
            pk_results = self.select_to_dict(pk_sql, table_name=db_table, owner=schema)
            if not pk_results:
                raise ValueError(f"Primary Key not found for {full_path}.")
            self._pk_cache[full_path] = [row['COLUMN_NAME'] for row in pk_results]
        return self._pk_cache[full_path]

    def select_to_dict(self, sql: Union[str, IO], *args, **kwargs) -> List[dict]:
        """
        Returns a python dictionary containing the results of the SELECT sql.

        :param sql: Query string OR a file-like object containing the SQL.
        :param args: Bind variables by position (optional)
        :param kwargs: Bind variables by name (optional)
        :return: List of dictionaries
        """
        # If the object has a 'read' method, extract the string content
        query = sql.read() if hasattr(sql, 'read') else sql

        if args or kwargs:
            result = self.cursor.execute(query, *args, **kwargs).fetchall()
        else:
            result = self.cursor.execute(query).fetchall()

        col_names = [col[0] for col in self.cursor.description]
        output = []
        for row in result:
            output.append({col_names[i]: row[i] for i in range(len(col_names))})
        return output

    def select_to_single_column_list(self, sql: str) -> list:
        result = self.cursor.execute(sql)
        return [row[0] for row in result]

    def insert_record(self, db_table: str, schema: Optional[str] = None, *args, **kwargs) -> dict:
        """
        ** Insert must be commited **
        Insert a new record into table provided given keys/values from kwargs. Values must be the actual
        row value (not a sql function). Passing sql functions is not currently supported.
        :param db_table: db table name
        :param schema: (Optional) Schema name
        :param args: values for new record (by position)
        :param kwargs: keys (aka column names) / values for new record (must match table columns)
        :return: dict containing inserted record
        """
        ret_rowid = self.cursor.var(oracledb.STRING)  # noqa
        full_path = f"{schema}.{db_table}" if schema else db_table

        if args:
            placeholders = ", ".join([f":{i + 1}" for i in range(len(args))])
            ret_idx = len(args) + 1
            insert_sql = f"INSERT INTO {full_path} VALUES ({placeholders}) RETURNING ROWID INTO :{ret_idx}"
            self.cursor.execute(insert_sql, list(args) + [ret_rowid])

        elif kwargs:
            insert_sql = f"""
                INSERT INTO {full_path} ({", ".join(kwargs.keys())}) VALUES ({":" + ", :".join(kwargs.keys())}) RETURNING ROWID INTO :ret_rowid
            """
            self.cursor.execute(insert_sql, {**kwargs, "ret_rowid": ret_rowid})
        else:
            raise TypeError("insert_new_record missing 1 required positional argument")

        _rowid = ret_rowid.getvalue()[0]
        self.cursor.execute(f"SELECT * FROM {full_path} WHERE ROWID = :1", [_rowid])
        # Set rowfactory to return a dict
        self.cursor.rowfactory = lambda *args: dict(zip([col[0] for col in self.cursor.description], args))
        return self.cursor.fetchone()

    def update_record(self, db_table: str, schema: Optional[str] = None, **setters) -> None:
        """
        ** Update must be commited **
        Insert a new record into table provided given keys/values from kwargs. Values must be the actual
        row value (not a sql function). Passing sql functions is not currently supported.
        :param db_table: db table name
        :param schema: (Optional) Schema name
        :param setters: keys (aka column names) / value updates for record (must match table columns)
        :return: None
        """
        db_pk_cols = self._pk_caching(db_table=db_table, schema=schema)
        full_path = f"{schema}.{db_table}" if schema else db_table

        setter_str = ", ".join([f"{k}=:{k}" for k in setters.keys() if k not in db_pk_cols])
        where_clause = " AND ".join([f"{pk} = :{pk}" for pk in setters.keys() if pk in db_pk_cols])
        update_sql = f"""
            UPDATE {full_path} SET {setter_str} WHERE {where_clause}
        """
        self.cursor.execute(update_sql, **setters)

    def insert_or_update_record(self, db_table: str, schema: Optional[str] = None, **record_data) -> dict:
        """
        Performs an UPSERT (MERGE) operation.
        - Automatically detects single or composite Primary Keys.
        - Only performs an UPDATE if the data has actually changed.
        - Handles optional schema prefixing.
        - Returns the final record as a dictionary.

        :param db_table: The name of the database table.
        :param schema: Optional schema name.
        :param record_data: Key-value pairs for the record (must include PK columns).
        :return: Dictionary of the inserted/updated record.
        """
        # Check Cache for PK metadata, otherwise fetch it
        db_pk_cols = self._pk_caching(db_table=db_table, schema=schema)
        full_path = f"{schema}.{db_table}" if schema else db_table

        # Map PKs (Find which user key matches the DB column)
        pk_map = {}
        for db_col in db_pk_cols:
            match = next((k for k in record_data if k.upper() == db_col), None)
            if not match:
                raise KeyError(f"PK column '{db_col}' missing from record_data.")
            pk_map[db_col] = match

        # SQL Construction
        update_keys = [k for k in record_data if k not in pk_map.values()]
        on_clause = " AND ".join([f"dest.{db} = :{usr}" for db, usr in pk_map.items()])

        update_section = ""
        if update_keys:
            set_stmt = ", ".join([f"dest.{k} = :{k}" for k in update_keys])
            change_stmt = " OR ".join([f"LNNVL(dest.{k} = :{k})" for k in update_keys])
            update_section = f"WHEN MATCHED THEN UPDATE SET {set_stmt} WHERE {change_stmt}"

        merge_sql = f"""
              MERGE INTO {full_path} dest
              USING dual ON ({on_clause})
              {update_section}
              WHEN NOT MATCHED THEN
                  INSERT ({', '.join(record_data.keys())}) 
                  VALUES ({', '.join(f':{k}' for k in record_data)})
          """
        try:
            # Execution
            self.cursor.execute(merge_sql, record_data)

            # Retrieval
            where_fetch = " AND ".join([f"{pk} = :{rpk}" for pk, rpk in pk_map.items()])
            blind_vars = {pk: record_data[rpk] for pk, rpk in pk_map.items()}

            self.cursor.execute(f"SELECT * FROM {full_path} WHERE {where_fetch}", blind_vars)
            self.cursor.rowfactory = lambda *args: dict(zip([c[0] for c in self.cursor.description], args))
            return self.cursor.fetchone()

        except oracledb.DatabaseError:
            self.rollback()
            raise

    def schema_builder(self, schema_name: str, table_name: str) -> Optional[dict]:
        schema = {}
        _sql = """
            SELECT table_name, column_name, data_type
            FROM all_tab_columns 
            WHERE upper(owner) = upper(:1)
            AND upper(table_name) = upper(:2)
            ORDER BY column_id
        """
        columns = self.select_to_dict(_sql, [schema_name, table_name])
        if not columns:
            return None
        schema[schema_name] = {table_name: {col['COLUMN_NAME']: col['DATA_TYPE'] for col in columns}}
        return schema

    def executemany(self, *args, **kwargs):
        """Simple wrapper for executemany method"""
        return self.cursor.executemany(*args, **kwargs)


def listagg_to_string_agg(expression: exp.Expression) -> exp.Expression:
    """
    Transforms LISTAGG to STRING_AGG using only generic classes (exp.Anonymous)
    to avoid AttributeErrors in very old sqlglot versions.
    """
    if isinstance(expression, exp.Expression) and str(expression.this).upper() == "LISTAGG":
        func_exp = expression.expressions[0]
        delimiter = expression.expressions[1]
        order_by_clause = expression.args.get("within_group")

        new_expressions = [func_exp, delimiter]
        new_args = {}

        if order_by_clause and isinstance(order_by_clause, exp.WithinGroup):
            order = order_by_clause.args.get("order")

            # For STRING_AGG in PostgreSQL dialect, ORDER BY is a keyword argument or simply attached to the expression in older sqlglot.
            order_node = exp.Order(this=order.this)
            new_args["order"] = order_node

        # Create the new exp.Anonymous expression, forcing the name to STRING_AGG
        string_agg_expression = exp.Anonymous(
            this="STRING_AGG",
            expressions=new_expressions,
            args=new_args
        )
        return string_agg_expression
    return expression


def convert_partition_to_where(expression):
    """
    Custom transformation to replace Oracle's PARTITION clause with a
    corresponding WHERE clause condition.
    """
    if isinstance(expression, exp.Select):
        from_exp = expression.args.get('from', None)
        if from_exp is not None:
            table_exp = from_exp.this if isinstance(from_exp, exp.From) else from_exp
            table_comments = getattr(table_exp, 'comments', []) or []
            table_alias_comments = getattr(table_exp.args.get('alias'), 'comments', []) or []
            all_comments = table_comments + table_alias_comments
            if all_comments:
                stripped_comments = [c.strip() for c in all_comments]
                if "{partition(popen_current)}" in stripped_comments:
                    new_condition = exp.condition("tripstatuscode = 'O'")
                    where_exp = expression.args.get('where')
                    if where_exp:
                        combined_condition = exp.And(this=where_exp.this, expression=new_condition)
                        expression.set("where", exp.Where(this=combined_condition))
                    else:
                        expression.set("where", exp.Where(this=new_condition))
    return expression


def case_sensitive_aliases(expression):
    """
    Transforms column aliases to use uppercase names, but preserves
    'TripStatusCode' alias.
    """
    # Define the alias to preserve (case-insensitive check is best practice)
    if isinstance(expression, exp.Alias):
        # The alias name is in the 'this' argument of the Alias expression
        col_name = expression.this.name
        if expression.args.get('alias'):
            if col_name == expression.alias:
                _alias = expression.alias.upper()
            else:
                _alias = expression.alias
            _alias = f"\"{_alias}\""
            expression.set("alias", None)
            new_alias_expression = exp.Alias(this=expression, alias=_alias)
            return expression.replace(new_alias_expression)
    return expression


def uppercase_top_level_aliases(expression):
    """
    Transforms column aliases to uppercase only in the top-level SELECT statement.
    """
    if isinstance(expression, exp.Select) and expression.parent is None:
        for projection in expression.expressions:
            if isinstance(projection, exp.Alias):
                alias_identifier = projection.args.get("alias")
                if alias_identifier and isinstance(alias_identifier, exp.Identifier):
                    if alias_identifier.args.get("quoted"):
                        return expression
                    new_alias_name = alias_identifier.this.upper()
                    new_alias = exp.to_identifier(
                        new_alias_name,
                        quoted=True
                    )
                    projection.set("alias", new_alias)
        return expression
    return expression


def transform_rownum_to_limit(expression: exp.Expression) -> exp.Expression:
    """
    Custom sqlglot transformation to convert Oracle's ROWNUM <= N
    in the WHERE clause into a PostgreSQL LIMIT N clause.

    This handles the most common use case of ROWNUM (limiting results).
    """
    if isinstance(expression, exp.Select):
        where = expression.args.get("where")

        if where and isinstance(where, exp.Where):
            condition = where.this

            if isinstance(condition, exp.LTE):
                left = condition.this
                right = condition.expression

                left_name = left.name.upper() if hasattr(left, 'name') else ''
                is_rownum_check = (
                        left_name == "ROWNUM" or
                        (isinstance(left, exp.Column) and left.alias_or_name.upper() == "ROWNUM")
                )
                if is_rownum_check:
                    try:
                        limit_value = right.copy()
                    except Exception:  # noqa
                        # If extraction fails, we can't safely proceed
                        return expression

                    expression = expression.copy()
                    expression.args.pop("where", None)

                    expression = expression.limit(limit_value)
                    return expression
    return expression



class OracleSQLParser:

    def __init__(self, sql_text, oracle_conn: OracleAPI = None):
        self.oracle = oracle_conn
        self.original = sql_text
        self.text = sql_text
        self._tablenames = None
        self._error_lvl = ErrorLevel.IGNORE
        self.transforms = [
            convert_partition_to_where,
            listagg_to_string_agg,
            transform_rownum_to_limit,
        ]
        self.post_transforms = [
            {"func": self.replace_case_insensitive, "args": ("DATE_TRUNC('DD'", "DATE_TRUNC('day'"), "kwargs": {}},
            {"func": self.replace_case_insensitive, "args": ("gvp.pkg_shipment.", "gvp."), "kwargs": {}}
        ]
        # sqlglot Expression
        self._exp = None

    @property
    def tablenames(self) -> set[tuple[str, str]]:
        if self._tablenames:
            return self._tablenames
        parsed_expression = parse_one(self.text, dialect='oracle', error_level=self.error_lvl)
        cte_names = {('', cte.alias_or_name) for cte in parsed_expression.find_all(exp.CTE)}
        table_names = {(table.db, table.name) for table in parsed_expression.find_all(exp.Table)}
        self._tablenames = table_names.difference(cte_names)
        return self._tablenames

    @property
    def expression(self) -> sqlglot.expressions.Expression:
        if self._exp is not None:
            return self._exp
        self._exp = parse_one(self.text, dialect="oracle", error_level=self.error_lvl)
        return self._exp

    @expression.setter
    def expression(self, exp):
        self._exp = exp

    @property
    def error_lvl(self) -> ErrorLevel:
        return self._error_lvl

    @error_lvl.setter
    def error_lvl(self, lvl: ErrorLevel):
        self._error_lvl = lvl

    def star_expansion(self):
        if not self.oracle:
            raise Exception("OracleAPI instance required when using star_expansion in OracleSQLParser")

        db_schema = {}
        for schema_name, tbl_name in self.tablenames:
            oracle_tb_schema = self.oracle.schema_builder(schema_name, tbl_name)
            if not oracle_tb_schema:
                continue
            if schema_name in db_schema:
                db_schema[schema_name].update(oracle_tb_schema[schema_name])
            else:
                db_schema[schema_name] = oracle_tb_schema[schema_name]

        self.expression = qualify(self.expression, schema=db_schema, expand_stars=True, quote_identifiers=False, validate_qualify_columns=False)
        self.text = self.expression.__str__()
        return self.text

    def transform(self, dialect: str, expand_stars: bool = True):
        if expand_stars:
            self.star_expansion()

        for func in self.transforms:
            self.expression = self.expression.transform(func)

        top_level_query = self.expression.find(exp.Select)
        if top_level_query:
            self.expression = top_level_query.transform(uppercase_top_level_aliases)

        self.text = self.expression.sql(dialect)
        self.post_transform()
        return self.text

    def post_transform(self):
        self._normalize_sql()
        # run any post transform cleanup here
        for t in self.post_transforms:
            self.text = t['func'](*t['args'], **t['kwargs'])

    def find_and_replace_schema(self, replacements: Tuple[Tuple[str, str], ...]) -> str:
        """
        Replaces words or phrases in a string using a single-pass regex search for efficiency,
        while honoring the quoting style of the original match.
        :param replacements: A tuple of tuples. Each inner tuple contains a (find_str, replace_str) pair.
                    The find_str should be the unquoted name (e.g., 'GVP.TABLE1' or 'GVP.').
        :return: modified string
        """
        all_find_strings = []
        canonical_replacement_map = {}

        for find_str, replace_str in replacements:
            # Normalize the find_str (remove dot, upper case) to create a canonical key for mapping
            canonical_key = find_str.rstrip('.').upper()
            canonical_replacement_map[canonical_key] = replace_str
            has_trailing_dot = find_str.endswith('.')
            find_str_clean = find_str.rstrip('.')
            all_find_strings.append(find_str)

            quoted_str_literal = ""
            if find_str_clean:
                parts = find_str_clean.split('.')
                # Quote each part (e.g., "GVP"."TABLE1")
                quoted_parts_list = [f'"{part}"' for part in parts]
                quoted_str_literal = '.'.join(quoted_parts_list)

            if has_trailing_dot:
                quoted_str_literal += '.'

            all_find_strings.append(quoted_str_literal)

        # CRITICAL: Sort pattern parts by length (longest first) to ensure multi-component names
        sorted_patterns = sorted(all_find_strings, key=len, reverse=True)
        combined_pattern = re.compile(fr'{"|".join(re.escape(p) for p in sorted_patterns)}', flags=re.IGNORECASE)

        def replacer(match):
            original_match = match.group(0)
            canonical_match_key = original_match.replace('"', '').rstrip('.').upper()
            replace_str = canonical_replacement_map.get(canonical_match_key)

            if replace_str is None:
                return original_match

            if '"' in original_match:
                has_trailing_dot = replace_str.endswith('.')
                replace_str_clean = replace_str.rstrip('.')
                new_parts = replace_str_clean.split('.')
                quoted_new_parts = [f'"{part}"' for part in new_parts]
                quoted_result = '.'.join(quoted_new_parts)
                return quoted_result + ('.' if has_trailing_dot else '')
            else:
                return replace_str

        self.text = combined_pattern.sub(replacer, self.text)
        return self.text

    def _normalize_sql(self):
        temp_text = re.sub(r'[\n\r\t]', '', self.text)
        self.text = re.sub(r'\s+', ' ', temp_text).strip()
        return self.text

    def replace_case_insensitive(self, literal_substring: str, replacement: str) -> str:
        escaped_pattern = re.escape(literal_substring)
        self.text = re.sub(escaped_pattern, replacement, self.text, flags=re.IGNORECASE)
        return self.text