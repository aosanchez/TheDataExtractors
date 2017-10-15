from sqlalchemy import create_engine


def inputTable():
    nameTable = input("Enter table name: ")
    readTable(nameTable)


def readTable(nameTable):
    stmt = 'SELECT * FROM ' + nameTable
    results = engine.execute(stmt)
    printResults(results)


def printResults(results):
    for result in results:
        print(result)


def trimPopString():
    stmt = "UPDATE population \
            SET census=trim(trailing ', District of Columbia, District of Columbia' from census)"
    engine.execute(stmt)


def trimPovString():
    stmt = "UPDATE poverty \
            SET census=trim(trailing ', District of Columbia, District of Columbia' from census)"
    engine.execute(stmt)


def joinPopByCensusTract():
    stmt = "SELECT population.year, population.census, population.pop, census_tract_coordinates.latitude, census_tract_coordinates.longitude \
            FROM population \
            LEFT JOIN census_tract_coordinates on population.census = census_tract_coordinates.census"
    results = engine.execute(stmt)
    printResults(results)


def joinPovByCensusTract():
    stmt = "SELECT poverty.year, poverty.census, poverty.pov, census_tract_coordinates.latitude, census_tract_coordinates.longitude \
            FROM poverty \
            LEFT JOIN census_tract_coordinates on poverty.census = census_tract_coordinates.census"
    results = engine.execute(stmt)
    printResults(results)


if __name__ == '__main__':
    engine = create_engine(
        'postgres://Jay:Huang@de-dbinstance.c6dfmakosb5f.us-east-1.rds.amazonaws.com:5432/dataextractorsDB')
    print(engine.table_names())

    inputTable()
    # joinPovByCensusTract()
    # joinPopByCensusTract()