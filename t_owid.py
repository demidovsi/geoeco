from owid import catalog
from owid.catalog import RemoteCatalog
from owid.catalog.tables import SCHEMA, Table
import pandas


# look for Covid-19 data, return a data frame of matches
# catalog.find('covid')

# load Covid-19 data from the Our World in Data namespace as a data frame
# df = catalog.find('population', namespace='owid').load()

# load data from other than the default `garden` channel
# lung_cancer_tables = catalog.find('lung_cancer_deaths_per_100000_men', channels=['open_numbers'])
# lung_cancer_tables = catalog.find('population')
# df = lung_cancer_tables.iloc[0].load()

# find the default OWID catalog and fetch the catalog index over HTTPS
# cat = catalog.RemoteCatalog()
cat = catalog.RemoteCatalog(channels=('garden', 'meadow', 'open_numbers'))

# get a list of matching tables in different datasets
matches = cat.find('population').iloc[0].load()

# fetch a data frame for a specific match over HTTPS
# t = cat.find('population', namespace='gapminder')
# t = cat.find('population')

# load other channels than `garden`
cat = RemoteCatalog(channels=('garden', 'meadow', 'open_numbers'))

# for col in t.all_columns:
#         t._fields[col] = mock(VariableMeta)