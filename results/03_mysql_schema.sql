-- Schema & Indexes for retail_sales
ALTER TABLE `retail_sales` ADD PRIMARY KEY (`Transaction ID`);
CREATE INDEX idx_customer ON `retail_sales` (`Customer ID`);
CREATE INDEX idx_date ON `retail_sales` (`Transaction Date`);
CREATE INDEX idx_category ON `retail_sales` (`Category`);
