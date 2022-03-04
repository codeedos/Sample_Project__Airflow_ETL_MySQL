

from faker import Faker
import faker_commerce
import random
import string
import pandas as pd
import os
import sqlalchemy as db


# ----------Define Parameters--------------

# define number of items to be generated
numSupplier = 100
numCompany = 100
numProduct = 1000
maxProductPrice = 100
numCustomer = 1000

# define number of supplier product prices
numFactSupplierProductPrice = 5000

# define number of company product prices
numFactCompanyProductPrice = 5000

# define number of orders
numFactOrder = 10000

# whether to export to CSV in the current directory
loadToMySQL = True


# ----------Generator code starts here-----------

faker = Faker()
# Generate Supplier and Company CUIT and names
supplierList = []
companyList = []
CUIT = []
coName = []
numSupplierAndCompany = numSupplier + numCompany
for _ in range(int(numSupplierAndCompany*1.25)):
    CUIT.append(random.choice(['20', '23', '24', '25', '26', '27',
                               '30', '33']) + ''.join(random.choices(string.digits, k=9)))
    coName.append(faker.company())

uniqueCUIT = list(set(CUIT))
uniqueCoName = list(set(coName))

supplierCUIT = []
supplierName = []
companyCUIT = []
companyName = []

supplierCUIT = uniqueCUIT[0:numSupplier]
supplierName = uniqueCoName[0:numSupplier]
companyCUIT = uniqueCUIT[numCompany:numCompany*2]
companyName = uniqueCoName[numCompany:numCompany*2]

for i in range(numSupplier):
    supplierList.append([supplierCUIT[i], supplierName[i]])
for i in range(numCompany):
    companyList.append([companyCUIT[i], companyName[i]])

# print(supplierList[:5])
# print(supplierCUIT[:5])
# print(companyCUIT[:5])
# print(supplierName[:5])
# print(companyName[:5])


# Generate Product names and prices

productList = []
productName = []
price = []
faker.add_provider(faker_commerce.Provider)
for _ in range(int(int(numProduct*1.25))):
    productName.append(faker.ecommerce_name())
for _ in range(numProduct):
    price.append(random.randint(1, maxProductPrice))

uniqueProductName = list(set(productName))[0:numProduct]
# print(productName[:5])
for i in range(numProduct):
    productList.append([productName[i], price[i]])
# print(productList[:5])


# Generate Customers
customerList = []
customerName = []
documentNumber = []
birthDate = []
for _ in range(int(numCustomer * 1.25)):
    documentNumber.append(faker.ssn())
    customerName.append(faker.name())
    birthDate.append(faker.date_time().strftime('%Y/%m/%d'))

uniqueDocumentNumber = list(set(documentNumber))[0:numCustomer]
uniqueCustomerNames = list(set(customerName))[0:numCustomer]

for i in range(numCustomer):
    customerList.append([documentNumber[i], customerName[i], birthDate[i]])
# print(customerList[:5])


# Generate factSupplierProductPrice
factSupplierProductPrice = {}

cnt = 0
while cnt < numFactSupplierProductPrice:
    supplierId = random.randint(0, numSupplier-1)
    productId = random.randint(0, numProduct-1)
    if (supplierId, productId) not in factSupplierProductPrice:
        factSupplierProductPrice[(supplierId, productId)
                                 ] = productList[productId][1] * (1+random.uniform(0, 1))

    cnt += 1

# print(len(factSupplierProductPrice))

factSupplierProductPriceList = []
for k in factSupplierProductPrice:
    factSupplierProductPriceList.append(
        [list(k)[0], list(k)[1], float("%.2f" % factSupplierProductPrice[k])])
    # print(str(list(k)[0]) + ", " + str(list(k)[1]) +
    #       ", " + str(factSupplierProductPrice[k]))
    # print(f"{list(k)[0]}, {list(k)[1]}, {v}")

# print(sorted(factSupplierProductPriceList[:5]))


# Generate factCompanyProductPrice
factCompanyProductPrice = {}
cnt = 0
while cnt < numFactCompanyProductPrice:
    companyId = random.randint(0, numCompany-1)
    supplierProductPrice = random.choice(list(factSupplierProductPrice.keys()))
    supplierId = supplierProductPrice[0]
    productId = supplierProductPrice[1]
    if (companyId, supplierId, productId) not in factCompanyProductPrice:
        factCompanyProductPrice[(companyId, supplierId, productId)
                                ] = factSupplierProductPrice[supplierProductPrice] * (1+random.uniform(0, 1)+random.uniform(0, 1))
    cnt += 1
# print(next(iter(factCompanyProductPrice)))

factCompanyProductPriceList = []
for k in factCompanyProductPrice:
    factCompanyProductPriceList.append(
        [list(k)[0], list(k)[1], list(k)[2], float("%.2f" % factCompanyProductPrice[k])])

# print(sorted(factCompanyProductPriceList[:5]))


# Generate factOrder
factOrder = {}
cnt = 0
companyIdList = [item[0] for item in factCompanyProductPriceList]
companyProductList = {}
for item in factCompanyProductPriceList:
    if item[0] not in companyProductList.keys():
        companyProductList[item[0]] = [item[2]]
    else:
        companyProductList[item[0]].append(item[2])

# print(len(companyProductList))

factOrderList = []
while cnt < numFactOrder:
    companyId = random.choice(list(companyProductList.keys()))
    productId = random.choice(companyProductList[companyId])
    customerId = random.randint(0, numCustomer)
    factOrderList.append([companyId, productId, customerId])
    cnt += 1
# print(factOrderList[:10])


# ------------------- Load to MySQL -----------------

engineB2B = db.create_engine(
    'mysql+mysqlconnector://airflow:airflow@localhost:3306/B2B', echo=False)

# dimCompany
df = pd.DataFrame(companyList, columns=['CUIT', 'name'])
df.to_sql(con=engineB2B, name='dimCompany',
          if_exists='append', index=False, chunksize=1000, method='multi')

# dimSupplier
df = pd.DataFrame(supplierList, columns=['CUIT', 'name'])
df.to_sql(con=engineB2B, name='dimSupplier',
          if_exists='append', index=False, chunksize=1000, method='multi')

# dimProduct
df = pd.DataFrame(productList, columns=['productName', 'price'])
df = df.drop(['price'], axis=1)
# print(df.head())
df.to_sql(con=engineB2B, name='dimProduct',
          if_exists='append', index=False, chunksize=1000, method='multi')

# dimCustomer
df = pd.DataFrame(customerList, columns=[
                  'documentNumber', 'fullname', 'dateOfBirth'])
df.to_sql(con=engineB2B, name='dimCustomer',
          if_exists='append', index=False, chunksize=1000, method='multi')

# factSupplierProductPrice
df = pd.DataFrame(factSupplierProductPriceList, columns=[
                  'supplierId', 'productId', 'price'])
df.to_sql(con=engineB2B, name='factSupplierProductPrice',
          if_exists='append', index=False, chunksize=1000, method='multi')

# factCompanyProductPrice
df = pd.DataFrame(factCompanyProductPriceList, columns=[
                  'companyId', 'supplierId', 'productId', 'price'])
df.to_sql(con=engineB2B, name='factCompanyProductPrice',
          if_exists='append', index=False, chunksize=1000, method='multi')

# factOrder
df = pd.DataFrame(factOrderList, columns=[
                  'companyId', 'productId', 'customerId'])
df.to_sql(con=engineB2B, name='factOrder',
          if_exists='append', index=False, chunksize=1000, method='multi')
