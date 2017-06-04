# Auto Backup Restore of RDS Instance & Redshift Cluster
This is a python script which is used to backup or restore the RDS instance & Redshift cluster when the EC2 on or off.

We used on demand RDS instance & Redshift cluster in our application which cost hourly basis.
As we are not using those resources 24/7 in the development phase. So we have developed a Lambda function
using python script which create the latest snapshot and delete the RDS instance or Redshift cluster
when EC2 is turning off and creating the RDS instance or Redshift cluster from the latest snapshot when EC2 has started.
We have created a Lambda function and invoke that function using Cloud watch rule.
