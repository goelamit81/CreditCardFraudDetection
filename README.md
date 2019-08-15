# CreditCardFraudDetection

As a big data engineer, architect and build a solution to cater to the following requirements:

## Fraud detection solution:

This is a feature to detect fraudulent transactions, wherein once a cardmember swipes his/her card for payment, the transaction should be classified as fraudulent or authentic based on a set of predefined rules. If fraud is detected, then the transaction must be declined. Please note that incorrectly classifying a transaction as fraudulent will incur huge losses to the company and also provoke negative consumer sentiment. 


## Customers' information:

The relevant information about the customers needs to be continuously updated on a platform from where the customer support team can retrieve relevant information in real time to resolve customer complaints and queries.

## Perform following tasks:

### Task 1: 
Load the transactions history data (card_transactions.csv) in a NoSQL database and create a look-up table with columns specified earlier in the problem statement in it.
### Task 2: 
Write a script to ingest the relevant data from AWS RDS to Hadoop.
### Task 3: 
Write a script to calculate the moving average and standard deviation of the last 10 transactions for each card_id for the data present in Hadoop and NoSQL database. If the total number of transactions for a particular card_id is less than 10, then calculate the parameters based on the total number of records available for that card_id. The script should be able to extract and feed the other relevant data (‘postcode’, ‘transaction_dt’, ‘score’, etc.) for the look-up table along with card_id and UCL.
### Task 4: 
Set up a job scheduler to schedule the scripts run after every 4 hours. The job should take the data from the NoSQL database and AWS RDS and perform the relevant analyses as per the rules and should feed the data in the look-up table.
### Task 5: 
Create a streaming data processing framework which ingests real-time POS transaction data from Kafka. The transaction data is then validated based on the three rules’ parameters (stored in the NoSQL database) discussed above.
### Task 6: 
Update the transactions data along with the status (Fraud/Genuine) in the card_transactions table.
### Task 7: 
Store the ‘postcode’ and ‘transaction_dt’ of the current transaction in the look-up table in the NoSQL database.
