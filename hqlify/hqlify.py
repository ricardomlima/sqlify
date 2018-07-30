"""

Program do abstract and automate hql code building
queries

@author Ricardo M. Lima <ricardolima89@gmail.com>

"""

import json


class Hqlify:

    config = None
    selects = []
    joins   = []

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
        main_table = self.config['main_table']
        select_statements = ','.join(self.selects)
        join_statements   = ','.join(self.joins)
        hql = "SELECT {} FROM {} {}".format(select_statements, main_table, join_statements)
        return hql

    def build_field_statement(self, table, column, alias=None, database=None):
        """
        Builds a field statement

        """

        if alias == None:
            alias = column

        if database == None:
            source = "{}".format(table)
        else:
            source = "{}.{}".format(database, table)

        field_statement = "{source}.{column} AS {alias}".format(
            **{"source": source, "column": column, "alias": alias})

        return field_statement

    def build_fields(self):
        """
        Build field queries

        """

        main_database = self.config['main_database']
        main_table    = self.config['main_table']
        reference_database = self.config['reference_database']
        fields = self.config['fields']

        for field in fields:

            if "on" in field:
                join_field = field["on"]
                reference_table = field["table"]

                self.build_join_statement(
                    main_table,
                    join_field,
                    reference_database,
                    reference_table
                )

            else:
                column = field["column"]
                if "alias" in field:
                    alias = field["alias"]
                else:
                    alias = column

                field_statement = self.build_field_statement(table=main_table, column=column, alias=column)
                self.selects.append(field_statement)

    def build_join_statement(self, main_table, join_field, reference_database, reference_table):
        reference_field = "CD_{}".format(reference_table)
        reference_description_field = "DESC_{}".format(reference_table)
        query_data = {
            "main_table": main_table,
            "join_field": join_field,
            "reference_database": reference_database,
            "reference_field": reference_field,
            "reference_table": reference_table
        }
        join_query  = "LEFT JOIN {reference_database}.{reference_table}"
        join_query += " ON {main_table}.{join_field} = {reference_database}.{reference_table}.{reference_field}"
        join_query = join_query.format(**query_data)

        self.joins.append(join_query)

        key_statement = self.build_field_statement(
            database=reference_database,
            table=reference_table,
            column=reference_field
        )

        self.selects.append(key_statement)

        description_statement = self.build_field_statement(
            database=reference_database,
            table=reference_table,
            column=reference_description_field
        )

        self.selects.append(description_statement)

if __name__ == "__main__":
    hqlify = Hqlify()
    hqlify.build_hql()
