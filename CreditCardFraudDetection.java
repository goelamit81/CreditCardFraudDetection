package com.upgrad.creditcardfrauddetection;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

@SuppressWarnings("serial")
public class CreditCardFraudDetection implements java.io.Serializable {

	/*
	 * Declare member variables for this class
	 */
	private String card_id = null;
	private String member_id = null;
	private double amount = 0.0;
	private String postcode = null;
	private String pos_id = null;
	private Date transaction_dt = null;
	private String status = null;

	private static Admin hBaseAdmin = null;
	public static String hostServerIP = null;

	/*
	 * Default constructor defined for this class
	 */
	public CreditCardFraudDetection() {
	}

	/*
	 * Parameterized constructor for this class. It takes the incoming record from
	 * DStream as JSON and parse it. This constructor initializes member variables
	 * to the values derived from parsing the JSON.
	 */
	public CreditCardFraudDetection(String jsonObj) {

		JSONParser parser = new JSONParser();
		try {

			JSONObject obj = (JSONObject) parser.parse(jsonObj);

			this.card_id = String.valueOf(obj.get("card_id"));
			this.member_id = String.valueOf(obj.get("member_id"));
			this.amount = Double.parseDouble(String.valueOf(obj.get("amount")));
			this.postcode = String.valueOf(obj.get("postcode"));
			this.pos_id = String.valueOf(obj.get("pos_id"));
			this.transaction_dt = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
					.parse(String.valueOf(obj.get("transaction_dt")));

		} catch (ParseException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * getHbaseAdmin method for setting up the HBase connection. hostServerIP is
	 * passed as command line argument and initialized by main method of
	 * KafkaConsumer class.
	 */
	public static Admin getHbaseAdmin() throws IOException {

		Configuration conf = HBaseConfiguration.create();
		conf.setInt("timeout", 120000);
		conf.set("hbase.master", hostServerIP + ":60000");
		conf.set("hbase.zookeeper.quorum", hostServerIP);
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent", "/hbase");
		try {
			Connection con = ConnectionFactory.createConnection(conf);
			if (hBaseAdmin == null) {
				hBaseAdmin = con.getAdmin();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return hBaseAdmin;

	}

	/*
	 * This is the method called by DStream records. It first classify transaction
	 * through classifyTransaction method. It then calls updateNoSQLDB method to
	 * update data in HBase tables.
	 */
	public void FraudDetection(CreditCardFraudDetection obj) {

		try {

			System.out.println("\n\n===================================================================");
			/*
			 * Print time when transaction processing started
			 */
			System.out.println("\nNew Transaction Processing Start Time : "
					+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

			System.out.println("\n======== Classify Current Transaction into GENUINE/FRAUD ==========");

			/*
			 * classifyTransaction method to validate transaction against various rules and
			 * classify current transaction into GENUINE/FRAUD
			 */
			classifyTransaction();

			System.out.println("\n========== Update NoSQL Database for Current Transaction ==========");

			/*
			 * updateNoSQLDB method to update NoSQL DB with card transaction details and if
			 * transaction is GENUINE, update lookup table as well
			 */
			updateNoSQLDB(obj);

			System.out.println("\n===================================================================");
			/*
			 * Print time when transaction processing finished
			 */
			System.out.println("\nCurrent Transaction Processing End Time : "
					+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

			System.out.println("\n===================================================================");

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/*
	 * Classify incoming transaction at POS as GENUINE or FRAUD
	 */
	private void classifyTransaction() throws IOException {

		/*
		 * Lets apply provided rules on each transaction:
		 * 
		 * 1. If member score is less than 200, then member could be a defaulter and
		 * possible case of fraud.
		 * 
		 * 2. If transaction amount for current transaction, exceeds UCL (upper control
		 * limit) based on last 10 genuine transactions then possible case of fraud.
		 * 
		 * 3. If speed of travel based on distance between postcode of current and last
		 * transaction, is more than 1km in 4sec = 0.25 km/sec then possible case of
		 * fraud.
		 */

		double card_ucl = 0;
		int card_score = 0;
		double speed = 0;

		/*
		 * Print Card ID of current transaction
		 */
		System.out.println("\nCard ID of current transaction is : " + this.getCard_id());

		/*
		 * Get UCL for Card ID
		 */
		card_ucl = getUCL(this.getCard_id());
		System.out.println("\nUCL (Upper Control Limit) for Card ID of current transaction is : " + card_ucl);

		/*
		 * Print current transaction amount
		 */
		System.out.println("\nCurrent Transaction Amount is : " + this.getAmount());

		/*
		 * Get score for Card ID
		 */
		card_score = getScore(this.getCard_id());
		System.out.println("\nCredit score for Card ID of current transaction is : " + card_score);

		/*
		 * Get speed of current transaction in relation to last transaction
		 */
		speed = getSpeed();

		/*
		 * All 3 rules need to be passed for card transaction status to be GENUINE
		 */
		if ((card_score >= 200) && (this.getAmount() <= card_ucl) && (speed <= 0.25)) {
			this.status = "GENUINE";
			System.out.println("\nCurrent Card Transaction has passed Credit Score Validation rule : Score : "
					+ card_score + " >= 200");
			System.out.println(
					"\nCurrent Card Transaction has passed UCL (Upper Control Limit) Validation rule : Amount : "
							+ this.getAmount() + " <= " + card_ucl);
			System.out.println(
					"\nCurrent Card Transaction has passed Zip Code Distance Validation rule : Speed (km/sec) : "
							+ speed + " <= 0.25");
			System.out.println("\nCurrent Card Transaction has passed all 3 Validation rules and so status is : "
					+ this.getStatus());
		} else {
			this.status = "FRAUD";
			if (card_score < 200) {
				System.out.println("\nCurrent Card Transaction did not pass through Credit Score Validation rule");
			}
			if (speed > 0.25) {
				System.out.println("\nCurrent Card Transaction did not pass through Zip Code Distance Validation rule");
			}
			if (this.getAmount() > card_ucl) {
				System.out.println(
						"\nCurrent Card Transaction did not pass through UCL (Upper Control Limit) Validation rule");
			}
			System.out.println("\nCurrent Card Transaction status is : " + this.getStatus());
		}
	}

	/*
	 * getUCL method to look up upper control limit for input Card ID from
	 * lookup_data_hive HBASE table
	 */
	public static Double getUCL(String cardID) throws IOException {

		if (cardID == null) {
			System.out.println("\nCard ID is not present in data recieved from Kafka. Kindly check Kafka stream");
			return -1d;
		} else {

			/*
			 * Call getHbaseAdmin() method only if connection is not already established
			 */
			if (hBaseAdmin == null) {
				hBaseAdmin = getHbaseAdmin();
			}

			/*
			 * Setup table name, get data for input card id and get ucl from result set. ucl
			 * belongs to lookup_card_family column family in look up table
			 * lookup_data_hive.
			 */
			String table = "lookup_data_hive";
			double val = 0;
			try {
				Get cardId = new Get(Bytes.toBytes(cardID));
				Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(table));
				Result result = htable.get(cardId);
				byte[] value = result.getValue(Bytes.toBytes("lookup_card_family"), Bytes.toBytes("ucl"));
				if (value != null) {
					val = Double.parseDouble(Bytes.toString(value));
				} else {
					val = 0d;
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

			return val;
		}
	}

	/*
	 * getScore method to look up score for input Card ID from lookup_data_hive
	 * HBASE table
	 */
	public static Integer getScore(String cardID) throws IOException {

		if (cardID == null) {
			System.out.println("\nCard ID is not present in data recieved from Kafka. Kindly check Kafka stream");
			return -1;
		} else {

			/*
			 * Call getHbaseAdmin() method only if connection is not already established
			 */
			if (hBaseAdmin == null) {
				hBaseAdmin = getHbaseAdmin();
			}

			/*
			 * Setup table name, get data for input card id and get score from result set.
			 * score belongs to lookup_card_family column family in look up table
			 * lookup_data_hive.
			 */
			String table = "lookup_data_hive";
			int val = 0;
			try {
				Get cardId = new Get(Bytes.toBytes(cardID));
				Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(table));
				Result result = htable.get(cardId);
				byte[] value = result.getValue(Bytes.toBytes("lookup_card_family"), Bytes.toBytes("score"));
				if (value != null) {
					val = Integer.parseInt(Bytes.toString(value));
				} else
					val = 0;

			} catch (Exception e) {
				e.printStackTrace();
			}

			return val;
		}
	}

	/*
	 * getSpeed method to calculate speed in km/sec based on distance between post
	 * code of current and last transaction. This method makes of DistanceUtility
	 * class provided
	 */
	private double getSpeed() {

		double distance = 0;
		long timeDifference = 0L;
		double speed = 0;

		try {

			/*
			 * Create an object of DistanceUtility class
			 */
			DistanceUtility distUtil = new DistanceUtility();

			/*
			 * Get last post code of Card ID from lookup table in HBase
			 */
			String lastPostCode = getPostCode(this.getCard_id());
			System.out.println("\nLast Post Code for Card ID of current transaction is : " + lastPostCode);

			/*
			 * Print post code of current transaction
			 */
			System.out.println("\nCurrent Post Code for Card ID of current transaction is : " + this.getPostcode());

			/*
			 * Get last transaction_dt of Card ID from Lookup table in HBase
			 */
			Date lastTransactionDt = getTransactionDate(this.getCard_id());
			System.out.println("\nLast Transaction Date for Card ID of current transaction is : "
					+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(lastTransactionDt));

			/*
			 * Print transaction date of current transaction
			 */
			System.out.println("\nCurrent Transaction Date for Card ID of current transaction is : "
					+ new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(this.getTransaction_dt()));

			/*
			 * Get distance between 2 post code, calculated by following method in
			 * DistanceUtility class. Distance is in kilometers.
			 */
			distance = distUtil.getDistanceViaZipCode(lastPostCode, this.getPostcode());
			System.out
					.println("\nDistance (returned by DistanceUtility) between current and last postcode (in km) is : "
							+ distance);

			/*
			 * Time difference between 2 transaction dates is calculated as below. The
			 * difference is returned in milliseconds so dividing by 1000 to get time in
			 * seconds. For many transactions, the incoming transaction date is less than
			 * last transaction date stored in lookup table so using abs method.
			 */
			timeDifference = (java.lang.Math.abs(this.getTransaction_dt().getTime() - lastTransactionDt.getTime()))
					/ 1000;
			System.out.println("\nAbsolute Time Difference between current and last transaction (in seconds) is : "
					+ timeDifference);

			/*
			 * Speed is being calculated in km/sec
			 * 
			 * At one instance, time difference was zero (2018-06-21 15:29:59) and last and
			 * current post code (55311) were same, distance utility returned non zero value
			 * (9.493073054631141E-5) which is quite small as such but still a non zero
			 * value was returned, due to which speed came out to be infinity for card_id =
			 * '4838289241690162'. So added this check if last and current post code are
			 * same then consider speed as 0.
			 */

			if (lastPostCode.equals(this.getPostcode())) {
				System.out.println(
						"\nSince Last Post Code and Current Post Code are same, so distance is 0 and so considering speed as 0");
				speed = 0;
			} else {
				speed = distance / timeDifference;
			}

			System.out.println("\nSpeed (Distance / Time) : " + speed + " km/second ");

		} catch (Exception e) {

			e.printStackTrace();
		}

		return speed;
	}

	/*
	 * getPostCode method to look up last post code for input Card ID from
	 * lookup_data_hive HBASE table
	 */
	public static String getPostCode(String cardID) throws IOException {

		if (cardID == null) {
			System.out.println("\nCard ID is not present in data recieved from Kafka. Kindly check Kafka stream");
			return null;
		} else {

			/*
			 * Call getHbaseAdmin() method only if connection is not already established
			 */
			if (hBaseAdmin == null) {
				hBaseAdmin = getHbaseAdmin();
			}

			/*
			 * Setup table name, get data for input card id and get postcode from result
			 * set. postcode belongs to lookup_transaction_family column family in look up
			 * table lookup_data_hive.
			 */
			String table = "lookup_data_hive";
			String val = null;
			try {
				Get cardId = new Get(Bytes.toBytes(cardID));
				Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(table));
				Result result = htable.get(cardId);
				byte[] value = result.getValue(Bytes.toBytes("lookup_transaction_family"), Bytes.toBytes("postcode"));
				if (value != null) {
					val = Bytes.toString(value);
				} else
					val = null;

			} catch (Exception e) {
				e.printStackTrace();
			}

			return val;
		}
	}

	/*
	 * getTransactionDate method to look up last transaction date for input Card ID
	 * from lookup_data_hive HBASE table
	 */
	public static Date getTransactionDate(String cardID) throws IOException {

		if (cardID == null) {
			System.out.println("\nCard ID is not present in data recieved from Kafka. Kindly check Kafka stream");
			return null;
		} else {

			/*
			 * Call getHbaseAdmin() method only if connection is not already established
			 */
			if (hBaseAdmin == null) {
				hBaseAdmin = getHbaseAdmin();
			}

			/*
			 * Setup table name, get data for input card id and get transaction_dt from
			 * result set. transaction_dt belongs to lookup_transaction_family column family
			 * in look up table lookup_data_hive.
			 */
			String table = "lookup_data_hive";
			Date val = null;
			try {
				Get cardId = new Get(Bytes.toBytes(cardID));
				Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(table));
				Result result = htable.get(cardId);
				byte[] value = result.getValue(Bytes.toBytes("lookup_transaction_family"),
						Bytes.toBytes("transaction_dt"));
				if (value != null) {
					val = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(Bytes.toString(value));
				} else
					val = null;

			} catch (Exception e) {
				e.printStackTrace();
			}
			return val;
		}
	}

	/*
	 * updateNoSQLDB method performs 2 activities:
	 * 
	 * 1. It inserts incoming card transaction details in card_transactions_hive
	 * HBASE table.
	 * 
	 * 2. It also updates lookup_data_hive HBASE table with current post code and
	 * transaction date if and only if the status of current transaction is GENUINE.
	 */
	public static void updateNoSQLDB(CreditCardFraudDetection transactionData) throws IOException {

		try {

			/*
			 * Call getHbaseAdmin() method only if connection is not already established
			 */
			if (hBaseAdmin == null) {
				hBaseAdmin = getHbaseAdmin();
			}

			/*
			 * Print Card ID of current transaction
			 */
			System.out.println("\nCard ID of current transaction is : " + transactionData.getCard_id());

			/*
			 * Convert incoming transaction date to specified format
			 */
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String val = dateFormat.format(transactionData.getTransaction_dt());

			Double Amount = transactionData.getAmount();

			String transactionTable = "card_transactions_hive";

			/*
			 * Check if table exists
			 */
			if (hBaseAdmin.tableExists(TableName.valueOf(transactionTable))) {
				Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(transactionTable));

				/*
				 * Use randomUUID as row key
				 */

				UUID transactionID = UUID.randomUUID();

				Put p = new Put(Bytes.toBytes(transactionID.toString()));

				/*
				 * Add column values for each column
				 */
				p.addColumn(Bytes.toBytes("card_transactions_family"), Bytes.toBytes("card_id"),
						Bytes.toBytes(transactionData.getCard_id()));
				p.addColumn(Bytes.toBytes("card_transactions_family"), Bytes.toBytes("member_id"),
						Bytes.toBytes(transactionData.getMember_id()));
				p.addColumn(Bytes.toBytes("card_transactions_family"), Bytes.toBytes("amount"),
						Bytes.toBytes(Amount.toString()));
				p.addColumn(Bytes.toBytes("card_transactions_family"), Bytes.toBytes("postcode"),
						Bytes.toBytes(transactionData.getPostcode()));
				p.addColumn(Bytes.toBytes("card_transactions_family"), Bytes.toBytes("pos_id"),
						Bytes.toBytes(transactionData.getPos_id()));
				p.addColumn(Bytes.toBytes("card_transactions_family"), Bytes.toBytes("transaction_dt"),
						Bytes.toBytes(val));
				p.addColumn(Bytes.toBytes("card_transactions_family"), Bytes.toBytes("status"),
						Bytes.toBytes(transactionData.getStatus()));

				htable.put(p);
				System.out.println("\nCurrent Transaction Details are populated in Card Transactions HBase Table : "
						+ transactionTable);
			} else {
				System.out.println("\nHBase Table named : " + transactionTable + " : does not exist");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		/*
		 * Check if status is Genuine to update lookup_data_hive HBase table
		 */
		if (transactionData.getStatus().equals("GENUINE")) {

			System.out.println("\nCurrent Card Transaction status is GENUINE so updating lookup table");

			try {

				/*
				 * Call getHbaseAdmin() method only if connection is not already established
				 */
				if (hBaseAdmin == null) {
					hBaseAdmin = getHbaseAdmin();
				}

				/*
				 * Convert incoming transaction date to specified format
				 */
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				String val = dateFormat.format(transactionData.getTransaction_dt());

				String lookupTable = "lookup_data_hive";

				/*
				 * Check if table exists
				 */
				if (hBaseAdmin.tableExists(TableName.valueOf(lookupTable))) {
					Table htable = hBaseAdmin.getConnection().getTable(TableName.valueOf(lookupTable));

					/*
					 * Instantiate Put class to Card ID row key in lookup_data_hive table
					 */
					Put p = new Put(Bytes.toBytes(transactionData.getCard_id()));

					/*
					 * Add column values for postcode and transaction_dt
					 */
					p.addColumn(Bytes.toBytes("lookup_transaction_family"), Bytes.toBytes("postcode"),
							Bytes.toBytes(transactionData.getPostcode()));
					p.addColumn(Bytes.toBytes("lookup_transaction_family"), Bytes.toBytes("transaction_dt"),
							Bytes.toBytes(val));

					/*
					 * Save put instance to HTable.
					 */
					htable.put(p);
					System.out.println(
							"\nPostcode and Transaction Date updated for Card ID of current transaction in lookup HBase table : "
									+ lookupTable);

				} else {
					System.out.println("\nHBase Lookup Table named : " + lookupTable + " : does not exist");
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("\nCurrent Card Transaction status is FRAUD so not updating lookup table");
		}

	}

	/*
	 * closeConnection method takes care of closing HBase connection
	 */
	public static void closeConnection() throws IOException {
		System.out.println("\nClosing the connection now...");
		hBaseAdmin.getConnection().close();
		System.out.println("\nConnection closed");

	}

	/*
	 * getter method for Card ID member variable1
	 */
	public String getCard_id() {
		return card_id;
	}

	/*
	 * getter method for member_id member variable
	 */
	public String getMember_id() {
		return member_id;
	}

	/*
	 * getter method for amount member variable
	 */
	public double getAmount() {
		return amount;
	}

	/*
	 * getter method for postcode member variable
	 */
	public String getPostcode() {
		return postcode;
	}

	/*
	 * getter method for pos_id member variable
	 */
	public String getPos_id() {
		return pos_id;
	}

	/*
	 * getter method for transaction_dt member variable
	 */
	public Date getTransaction_dt() {
		return transaction_dt;
	}

	/*
	 * getter method for status member variable
	 */
	public String getStatus() {
		return status;
	}

}
