"""

Program do abstract and automate hql code building
queries

@author Ricardo M. Lima <ricardolima89@gmail.com>

"""


class Hqlify:

    config_file = None

    def __init__(self):
        """
        Configuration loading

        """

        with open('config.json', 'r') as json_file:
            self.config_file = json_file.read()

    def build_hql(self):
        """
        Execute steps to build HQL
        """

        hql = "
        SELECT O.CMPT AS CMPT FROM DADOS_BRUTOS_OBS.ANSRESSARCIMENTO AS O
        "
        return hql

if __name__ == "__main__":
    hqlify = Hqlify()
