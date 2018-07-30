"""

Program do abstract and automate hql code building
queries

@author Ricardo M. Lima <ricardolima89@gmail.com>

"""

import json


class Hqlify:

    config = None
    selects = []
    froms = []
    joins = []
    alias_table = {}

    def __init__(self):
        """
        Configuration loading

        """

        with open('config.json', 'r') as json_file:
            self.config = json.loads(json_file.read())

    def build_hql(self):
        """
        Execute steps to build HQL

        """

        self.build_fields()
        main_database = self.config['main_database']
        main_table = self.config['main_table']

        main_from_statement = self.get_source_alias_statement(main_database, main_table)
        self.froms.append(main_from_statement)

        select_statements = ','.join(self.selects)
        froms_statements = ','.join(self.froms)
        join_statements = ' '.join(self.joins)

        hql = "SELECT {} FROM {} {}".format(
            select_statements, froms_statements, join_statements)

        return hql

    def build_field_statement(self, database, table, column, alias=None):
        """
        Builds a field statement

        """

        if alias == None:
            alias = column

        source_alias = self.get_source_alias(database, table)

        field_statement = "{source_alias}.{column} AS {alias}".format(
            **{"source_alias": source_alias, "column": column, "alias": alias})

        return field_statement

    def get_source_alias(self, database, table, alias=None):
        """
        Map a databases table to an alias

        """

        if database in self.alias_table:
            database_map = self.alias_table[database]
        else:
            database_map = {}
            self.alias_table[database] = database_map

        if table in database_map:
            source_alias = database_map[table]
        else:
            source_alias = table
            if alias != None:
                source_alias = alias
            self.alias_table[database][table] = source_alias

        return source_alias

    def check_table_alias_exists(self, database, table):
        """
        Check if alias exists

        """

        if database in self.alias_table:
            if table in self.alias_table[database]:
                return True

        return False

    def get_source_alias_statement(self, database, table):
        source_alias = self.get_source_alias(database, table)
        source_alias_statement_params = {
            "database": database,
            "table": table,
            "source_alias": source_alias,
        }
        statement = "{database}.{table} AS {source_alias}".format(
            **source_alias_statement_params)

        return statement

    def build_fields(self):
        """
        Build field queries

        """

        main_database = self.config['main_database']
        main_table = self.config['main_table']
        reference_database = self.config['reference_database']
        fields = self.config['fields']

        for field in fields:

            if "on" in field:
                join_field = field["on"]
                reference_table = field["table"]
                field_selector = field["field"] if "field" in field else None
                alias = field["alias"] if "alias" in field else None

                self.build_join_statement(
                    main_database,
                    main_table,
                    join_field,
                    reference_database,
                    reference_table,
                    field_selector,
                    alias=alias,
                )

            else:

                column = field["column"]
                if "alias" in field:
                    alias = field["alias"]
                else:
                    alias = column

                field_statement = self.build_field_statement(
                    database=main_database, table=main_table, column=column, alias=alias)
                self.selects.append(field_statement)

    def build_join_statement(self, main_database, main_table, join_field, reference_database, reference_table, field=None, alias=None):
        """
        Build join statements along with
        their respective select statements

        """

        field_selector = reference_table if field is None else field
        alias = field_selector if alias is None else alias
        reference_field_placeholder = "cd_{}"
        description_field_placeholder = "desc_{}"

        reference_field = reference_field_placeholder.format(field_selector)
        reference_description_field = description_field_placeholder.format(field_selector)

        reference_source_alias = self.get_source_alias(reference_database, reference_table)
        reference_source_alias_statement = self.get_source_alias_statement(reference_database, reference_table)

        main_table_alias = self.get_source_alias(main_database, main_table)

        query_data = {
            "main_table_alias": main_table_alias,
            "join_field": join_field,
            "reference_field": reference_field,
            "reference_source_alias_statement": reference_source_alias_statement,
            "reference_source_alias": reference_source_alias,
        }

        join_query = "LEFT JOIN {reference_source_alias_statement}"
        join_query += " ON {main_table_alias}.{join_field} = {reference_source_alias}.{reference_field}"
        join_query = join_query.format(**query_data)

        self.joins.append(join_query)

        reference_field_alias = reference_field_placeholder.format(alias)

        key_statement = self.build_field_statement(
            database=reference_database,
            table=reference_table,
            column=reference_field,
            alias=reference_field_alias,
        )

        self.selects.append(key_statement)

        description_field_alias = description_field_placeholder.format(alias)

        description_statement = self.build_field_statement(
            database=reference_database,
            table=reference_table,
            column=reference_description_field,
            alias=description_field_alias,
        )

        self.selects.append(description_statement)


if __name__ == "__main__":
    hqlify = Hqlify()
    hql = hqlify.build_hql()
    print(hql)
