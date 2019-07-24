# Address Normalizer / Fuzzy Matching 

## Set Up PostgreSQL and Postal Connection

linux / mac / or use VM in windows

Background Installation: needs Postgresql developer and CURL packages.

```bash
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib
sudo apt-get install postgresql-server-dev-11
sudo apt-get install libcurl4-openssl-dev

```

Setup PostgreSQL users / permissions - my username is *azad*, and the database I will be using is *azaddb*

```
sudo su - postgres -c "createuser azad"
sudo su - postgres -c "createdb azaddb"
```

Alter Role of Users postgres and personal account, you need a password to ssl into your database from another language or program if you need it later.

```bash
sudo -u postgres psql
#\ ALTER USER postgres WITH password 'your-pass';
```

```
#\  ALTER USER azad WITH SUPERUSER;
```

Quit and enter database with your account

```
sudo -u azad psql azaddb
```

 (in psql) #\ ALTER USER user_name WITH PASSWORD 'new_password';

```psql

ALTER USER azad WITH PASSWORD 'XXXXX';
```



#### Peer Authentication / MD5 / Auth Issues

Setup Access and auth to PostgreSQL DB connections by editing <u>pg_hba.conf</u> for local access

```
locate pg_hba.conf

# /etc/postgresql/11/main/pg_hba.conf

```

Change the line in <u>pg_hba.conf</u> from

```
local   all             postgres                                trust
```

to

```
local   all             postgres                                md5
```



### Install Postal and Psql-postal

PSQL-Postal the bindings for postal are located: <https://github.com/pramsey/pgsql-postal>

I recall the instructions for installation of postal being straight-forward. 

Then install the psql bindings

```bash
git clone <https://github.com/pramsey/pgsql-postal>
cd pgsql-postal
make
make install
```

Then install extension in your psql database

```bash
sudo psql -d azaddb -c 'create extension postal' azad -d azaddb -c 'create extension fuzzystrmatch' 
```



### Scripting Language to Use Your Psql-postal database

You can use your scripting language of choice, or psql itself to write your data into your data-base. I use R and R-Studio

```bash
sudo apt-get install r-base-core libssl-dev
```

  

### PSQL Memory Options

Random memory tips - you will need lots of memory for read ins particularly in your root filesystem where the database will be located. 

I wouldn't mess with **tablespaces** unless you know what you're doing.

#### psql database memory tunings

Basic Tunings to build on for your system can be found here, <https://pgtune.leopard.in.ua/#/>

Turn on huge_pages

```bash
psql -c "alter system set huge_pages=on" postgres
```

 Edit memory options: 

```
sudo pico /etc/postgresql/10/main/postgresql.conf
```

Lower or turn off swap since this is mostly ram intensive

```bash
sudo bash -c "echo 'vm.swappiness = 15' >> /etc/sysctl.conf" 
sudo sysctl -p
```

