# Homework: Week 6

## Question 1. Select the statements that are correct

:white_check_mark:• Kafka Node is responsible to store topics \
:white_check_mark:• Zookeeper is removed form Kafka cluster starting from version 4.0 \
:white_check_mark:• Retention configuration ensures the messages not get lost over specific period of time. \
:white_check_mark:• Group-Id ensures the messages are distributed to associated consumers

<br>

## Question 2. Please select the Kafka concepts that support reliability and availability

:white_check_mark:• Topic Replication \
• Topic Paritioning \
• Consumer Group Id \
:white_check_mark:• Ack All 

<br>

## Question 3: Please select the Kafka concepts that support scaling

• Topic Replication \
:white_check_mark:• Topic Partitioning \
:white_check_mark:• Consumer Group Id \
• Ack All

<br>

## Question 4. Please select the attributes that are good candidates for partitioning key. Consider cardinality of the field you have selected and scaling aspects of your application

• payment_type \
:white_check_mark:• vendor_id \
• passenger_count \
• total_amount \
• tpep_pickup_datetime \
• tpep_dropoff_datetime 

<br>

## Question 5.  Which configurations below should be provided for Kafka Consumer but not needed for Kafka Producer

:white_check_mark:• Deserializer Configuration \
:white_check_mark:• Topics Subscription \
• Bootstrap Server \
:white_check_mark:• Group-Id \
:white_check_mark:• Offset \
• Cluster Key and Cluster-Secret 

<br>

## Question 6. Please implement a streaming application, for finding out popularity of PUlocationID across green and fhv trip datasets. 

(no code)