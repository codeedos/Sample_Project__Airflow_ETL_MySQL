
CREATE DATABASE B2B;

USE B2B;

DROP TABLE IF EXISTS factSupplierProductPrice;
DROP TABLE IF EXISTS factCompanyProductPrice;
DROP TABLE IF EXISTS factOrder;
DROP TABLE IF EXISTS dimCustomer;
DROP TABLE IF EXISTS dimProduct;
DROP TABLE IF EXISTS dimSupplier;
DROP TABLE IF EXISTS dimCompany;
DROP TABLE IF EXISTS weblogs;

CREATE TABLE dimCustomer
(
    id INT NOT NULL
    AUTO_INCREMENT PRIMARY KEY,
    documentNumber VARCHAR
    (255) NULL,
    fullname VARCHAR
    (255) NULL,
    dateOfBirth DATE NULL
);

    CREATE TABLE dimProduct
    (
        id INT NOT NULL
        AUTO_INCREMENT PRIMARY KEY,
    productName VARCHAR
        (255) NULL
);

        CREATE TABLE dimSupplier
        (
            id INT NOT NULL
            AUTO_INCREMENT PRIMARY KEY,
    CUIT VARCHAR
            (255) NOT NULL,
    name VARCHAR
            (255) NULL
);

            CREATE TABLE dimCompany
            (
                id INT NOT NULL
                AUTO_INCREMENT PRIMARY KEY,
    CUIT VARCHAR
                (255) NOT NULL,
    name VARCHAR
                (255) NULL
);

                CREATE TABLE factSupplierProductPrice
                (
                    id INT NOT NULL
                    AUTO_INCREMENT PRIMARY KEY,
    supplierId INT NOT NULL,
    productId INT NOT NULL,
    price DOUBLE NULL
    
    FOREIGN KEY
                    (supplierId)
        REFERENCES dimSupplier
                    (id),
	
    FOREIGN KEY
                    (productId)
        REFERENCES dimProduct
                    (id)
        
);

                    CREATE TABLE factCompanyProductPrice
                    (
                        id INT NOT NULL
                        AUTO_INCREMENT PRIMARY KEY,
    companyId INT NOT NULL,
    supplierId INT NOT NULL,
    productId INT NOT NULL,
    price DOUBLE NULL
    
    FOREIGN KEY
                        (companyId)
        REFERENCES dimCompany
                        (id),
    
    FOREIGN KEY
                        (supplierId)
        REFERENCES dimSupplier
                        (id),
    FOREIGN KEY
                        (productId)
        REFERENCES dimProduct
                        (id)
     
);

                        CREATE TABLE factOrder
                        (
                            id INT NOT NULL
                            AUTO_INCREMENT PRIMARY KEY,
    companyId INT NOT NULL,
    productId INT NOT NULL,
    customerId INT NOT NULL
	
    FOREIGN KEY
                            (companyId)
        REFERENCES dimCompany
                            (id),
    
    FOREIGN KEY
                            (productId)
        REFERENCES dimProduct
                            (id),
        
    FOREIGN KEY
                            (customerId)
        REFERENCES dimCustomer
                            (id)
        
);
