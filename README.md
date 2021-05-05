# DIA Project- Implementation of MapReduce on Online Streaming Movies and TV Shows Dataset

## Structure of Project Folder

Python is used in this project. Mention below is structure of project
```
├── files
│   ├── MoviesOnStreamingPlatforms_updated.csv
│   ├── tv_shows.csv
├── ott.py
├── requirements.txt
├── README.md
```

## Prerequisites
Install following required tools
* Java
* Hadoop
* Spark
* pip3 to install python dependencies

Mentioned below are the steps to install requirements of the project
### Install Java

At the terminal (console) of the virtual machine, execute each of the following commands in turn. Press Enter after
typing each command. This will apply to all commands entered at the terminal.

```
sudo apt update
sudo apt install default-jre
sudo apt install default-jdk
```

When prompted, type y to confirm that you wish to install the packages. Note that the command sudo runs any commands
following it with superuser privileges.

### Create a new user and group to run Hadoop

Enter the command below to add a new user-group to your system. The group is called hadoop.

```
sudo groupadd hadoop
```

Next create a new user, adding it to the group we just created. The username is hduser.

```
sudo useradd -ghadoop hduser -m -s /bin/bash
```

The newly-created user will need to have a password set. The command below will set the password for the user hduser.
When prompted, enter the password hadoop and confirm it. Please do not change this password, as it will help ensure that
your use of the VM can be easily supported during lab sessions.

```
sudo passwd hduser
```

Finally, the newly-created user will need to be able to avail of superuser privileges. Run the following command to add
the user to the group with those privileges.

```
sudo usermod -aG sudo hduser Use the package manager pip to install dependencies
```

### Set up passphrase-less SSH

You will be setting up Hadoop to run in pseudo-distributed mode. This is where Hadoop launches separate process to
imitate running across multiple interconnected nodes. In a distributed environment, Hadoop uses the secure shell
protocol (SSH) to communicate with and manage its nodes.

We first need to log in to the hduser account. Do this by typing the following command.

```
su - hduser
```

The next step is to create a SSH (passphrase-less) key pair for the hduser account.

```
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
```

The newly generated keys are then copied to hduser's authorized keys using the command below.

```
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
```

In order to ensure that everything went well, we test that passphrase-less SSH works using the following command.

The first time you run this command you will be asked to accept the key fingerprint. Do so and you should find yourself
back at the prompt.

```
ssh localhost
```

At this point you should be logged in to the hduser account using SSH. You should now exit from the SSH shell session
using the command below.

```
exit
```

Note: If are prompted for a password when executing the ssh localhost command then SSH is not configured correctly. You
should revisit all steps in this section until SSH works as expected.

### Disable IPv6

Hadoop does not currently run over IPv6 networks, so we need to disable IPv6 using the command below.

```
sudo nano /etc/sysctl.conf
```

This will open a text editor. If you are a Linux pro you can, of course, use your favorite editor instead (e.g., vi).

Add the following entries at the end of the file and then save the file by pressing Ctrl and O simultaneously.

```
net.ipv6.conf.all.disable_ipv6=1
net.ipv6.conf.default.disable_ipv6=1
net.ipv6.conf.lo.disable_ipv6=1
```

Exit the editor by pressing Ctrl and X simultaneously.

To make these changes take immediate effect, run the following command:

```
sudo sysctl -p
```

### Install Hadoop

#### Download Hadoop

We will use the Hadoop 3.2.2. To download the Hadoop archive, type the following commands in the terminal and press
Enter after each one.

```
cd ~
curl -O https://downloads.apache.org/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz
```

If curl is not found on your system, you may need to install it with the command below:

```
sudo apt install curl
```

#### Verify the downloaded archive file

Checking the integrity of the downloaded archive is optional but highly recommended

Do this by entering the following commands.

```
cd ~
curl -O https://downloads.apache.org/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz.sha512
shasum -a 512 ./hadoop-3.2.2.tar.gz
```

Compare the output of this command with the line beginning with SHA512= in the file hadoop-3.2.2.tar.gz.sha512. If the
two match, the integrity of the archive can be assured. Note that you can open the sha512 file with any text editor.

#### Unpack the downloaded archive file

We will now change the working directory to where we want to install Hadoop by entering the following into the terminal
and pressing Enter. This is where we will install all downloaded software.

```
cd /usr/local
```

Next we unpack the archive we downloaded earlier. Type the two lines below into the terminal, pressing Enter after each
as usual. The second line removes (deletes) the downloaded archive file as it is no longer needed.

```
sudo tar xvfz ~/hadoop-3.2.2.tar.gz
rm ~/hadoop-3.2.2.tar.gz
```

The command tar followed by x unpacks an archive, much in the same way as zip archives are extracted on Windows or
MacOS. The tilde ~ is an alias for the current user's home directory.

#### Set permissions and create a symbolic link

Create a symbolic link (a form of shortcut) to alias the directory /usr/local/hadoop with /usr/local/hadoop-3.2.2. This
allows us to potentially have multiple versions of Hadoop installed and to switch between the by simply deleting and
recreating the symbolic link.

```
sudo ln -s hadoop-3.2.2 hadoop
```

Last of all, we run a command to ensure that all files in the hadoop directories are owned by the user hduser and the
group hadoop.

```
sudo chown -R hduser:hadoop hadoop*
```

The command chown changes the ownership of a file or directory. The asterisk after the word hadoop means that it will
operate on any file or directory starting with that word.

### Configure Hadoop

First change to the directory where the Hadoop configuration files are stored. This can be done either by issuing a
change directory command relative to the current directory (/usr/local) as shown below:

```
cd hadoop/etc/hadoop
```

or by entering the full path (an absolute path) as in the following example.

```
cd /usr/local/hadoop/etc/hadoop
```

Either one will do the job, but you will find relative paths easier to type, provided you know where the destination
directory is relative to your current directory.

Having changed directory, the first task is to ensure Hadoop knows where to find the Java Virtual Machine.

First make a copy of the MapReduce configuration template.

```
cp mapred-site.xml mapred-site.xml.template
```

Next a text editor to edit the mapred-site.xml file. Here we are using nano but you can use any other suitable text
editor.

```
nano mapred-site.xml
```

Edit the file to ensure the following configuration entries are present and then save the file.

```
<configuration>
  <property>
    <name>mapreduce.jobtracker.address</name>
    <value>local</value>
  </property>
</configuration>
```

Edit the core-site.xml file with the following configuration entries and save the file.

```
<configuration>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>/home/hduser/tmp</value>
  </property>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:54310</value>
  </property>
</configuration>
```

Edit the hdfs-site.xml file with the following configuration entries and save the file.

```
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/home/hduser/hdfs</value>
  </property>
</configuration>
```

#### Set the environmental variable for the Java home directory

First of all, you will need to identify the directory where Java is installed, by running the command below:

```
sudo update-alternatives --config java
```

You should have only one entry for java. Take a note of this, taking care to remove the trailing /bin/java or
jre/bin/java. So the following path returned by the command.

```
/usr/lib/jvm/java-11-openjdk-amd64/bin/java
```

should be recorded as:

```
/usr/lib/jvm/java-11-openjdk-amd64
```

Next use a text editor to edit the hadoop-env.sh file, changing the line starting with export JAVA HOME=, putting the
path to your Java installation immediately after the equals symbol and then save the file. For the example above, this
would be:

```
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

Note that the version of Java installed on your virtual machine may be different!

#### Set hduser environment variables

Make sure the environment variables HADOOP_CLASSPATH, HADOOP_MAPRED_HOME, HADOOP_HDFS_HOME, JAVA_HOME, and PATH are set.
Open the file with nano (or your editor of choice):

```
nano ~/.bashrc
```

Include the following statements at the end of the .bashrc file in your hduser home directory.

```
export HADOOP_CLASSPATH=/usr/lib/jvm/java-11-openjdk-amd64/lib/tools.jar:/usr/local/hadoop/bin/hadoop
export HADOOP_MAPRED_HOME=/usr/local/hadoop
export HADOOP_HDFS_HOME=/usr/local/hadoop
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$PATH:.:/usr/lib/jvm/java-11-openjdk-amd64/bin:/usr/local/hadoop/bin
```

Then, apply the changes by sourcing the file

```
source ~/.bashrc
```

Finally, check the above variables have been all initialized correctly using:

```
env | grep HADOOP
env | grep JAVA
```

#### Create a data directory

In the previous section, we specified the locations of two directories where Hadoop will store its files. We now need to
create them and set the access permissions for one of the two.

```
mkdir ~/tmp
mkdir ~/hdfs
chmod 750 ~/hdfs
```

The command chmod changes the access permissions for a file or directory. In octal mode, it takes the following
arguments:

```
chmod [options] [octalmode] [fileordir]
where [octalmode] is a three digit number, the leftmost digit indicates the permissions for the user, the middle digit for the group and the rightmost for all other users. Each digit can take a value between 0 and 7. In octal mode:
```

4 stands for read; 2 stands for write; 1 stands for execute; 0 stands for none; This can be combined to give a range of
values from 0 to 7. The meaning of each value is given in the table on the next page.

### Format the HDFS namenode

We will now format the HDFS namenode.

```
cd /usr/local/hadoop
bin/hdfs namenode -format
```

#### Start Hadoop service

```
sbin/start-dfs.sh
sbin/start-yarn.sh
```

Using the Java Process Status tool (JPS) we can check that everything is running.

```
jps
```

#### Create user directory

```
bin/hdfs dfs -mkdir /user
bin/hdfs dfs -mkdir /user/hduser
```

### Install and Configure Spark

1. Login to Ubuntu using the hduser account
2. As Spark is written in scala so scale must be installed to run spark on your machine

```
sudo apt-get install scala
```

3. Download the Spark

```
wget https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz
```

4. Uncompress the spark-3.1.1-bin-hadoop3.2.tgz using the command

```
tar xvzf spark-3.1.1-bin-hadoop3.2.tgz
```

5. Move the Spark installation to the /usr/local directory

```
sudo mv spark-3.1.1-bin-hadoop3.2 /usr/local/spark-3.1.1
sudo ln -s /usr/local/spark-3.1.1 /usr/local/spark
```

6. Change ownership of the directories to the hduser

```
sudo chown -R hduser:hadoop spark*
```

7. To communicate with the YARN Resource Manager, Spark needs to be aware of your Hadoop configuration. This is done via
   the HADOOP_CONF_DIR environment variable. The SPARK_HOME variable is not mandatory, but is useful when submitting
   Spark jobs from the command line.

```
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export LD_LIBRARY_PATH=/usr/local/hadoop/lib/native:$LD_LIBRARY_PATH

export SPARK_HOME=/usr/local/spark
export PYSPARK_PYTHON=python3
```

8. Add the Spark binaries directory /usr/local/spark/bin to your PATH in the .bashrc file. For example,

```
export PATH=$SPARK_HOME/bin:$PATH
```

9. Restart your session by logging out and logging in again.

### Install python dependencies

```
sudo apt-apt install python3-pip
pip3 install -r requirements.txt
```

## Run project

```
spark-submit ott.py
```
