-- Normalized Database Schema for Retail Sales
-- Generated for database: retail_db

-- Categories table

    CREATE TABLE IF NOT EXISTS `Categories` (
        `CategoryID` INT AUTO_INCREMENT PRIMARY KEY,
        `CategoryName` VARCHAR(64) NOT NULL UNIQUE,
        `CreatedAt` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        `UpdatedAt` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    

-- Locations table

    CREATE TABLE IF NOT EXISTS `Locations` (
        `LocationID` INT AUTO_INCREMENT PRIMARY KEY,
        `LocationName` VARCHAR(32) NOT NULL UNIQUE,
        `CreatedAt` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        `UpdatedAt` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    

-- PaymentMethods table

    CREATE TABLE IF NOT EXISTS `PaymentMethods` (
        `PaymentMethodID` INT AUTO_INCREMENT PRIMARY KEY,
        `PaymentMethodName` VARCHAR(32) NOT NULL UNIQUE,
        `CreatedAt` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        `UpdatedAt` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    

-- Customers table

    CREATE TABLE IF NOT EXISTS `Customers` (
        `CustomerID` VARCHAR(32) PRIMARY KEY,
        `CreatedAt` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        `UpdatedAt` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    

-- Items table

    CREATE TABLE IF NOT EXISTS `Items` (
        `ItemID` INT AUTO_INCREMENT PRIMARY KEY,
        `ItemName` VARCHAR(64) NOT NULL,
        `PricePerUnit` DECIMAL(10, 2) NOT NULL,
        `CategoryID` INT NOT NULL,
        `CreatedAt` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        `UpdatedAt` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (`CategoryID`) REFERENCES `Categories`(`CategoryID`) ON DELETE RESTRICT ON UPDATE CASCADE,
        UNIQUE KEY `unique_item_category` (`ItemName`, `CategoryID`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    

-- Transactions table

    CREATE TABLE IF NOT EXISTS `Transactions` (
        `TransactionID` VARCHAR(32) PRIMARY KEY,
        `CustomerID` VARCHAR(32) NOT NULL,
        `ItemID` INT NOT NULL,
        `PaymentMethodID` INT NOT NULL,
        `LocationID` INT NOT NULL,
        `Quantity` INT NOT NULL,
        `TotalPrice` DECIMAL(10, 2) NOT NULL,
        `TransactionDate` DATE NOT NULL,
        `DiscountApplied` BOOLEAN DEFAULT FALSE,
        `CreatedAt` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        `UpdatedAt` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (`CustomerID`) REFERENCES `Customers`(`CustomerID`) ON DELETE RESTRICT ON UPDATE CASCADE,
        FOREIGN KEY (`ItemID`) REFERENCES `Items`(`ItemID`) ON DELETE RESTRICT ON UPDATE CASCADE,
        FOREIGN KEY (`PaymentMethodID`) REFERENCES `PaymentMethods`(`PaymentMethodID`) ON DELETE RESTRICT ON UPDATE CASCADE,
        FOREIGN KEY (`LocationID`) REFERENCES `Locations`(`LocationID`) ON DELETE RESTRICT ON UPDATE CASCADE,
        INDEX `idx_customer` (`CustomerID`),
        INDEX `idx_item` (`ItemID`),
        INDEX `idx_date` (`TransactionDate`),
        INDEX `idx_payment_method` (`PaymentMethodID`),
        INDEX `idx_location` (`LocationID`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    

