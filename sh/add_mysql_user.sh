username=stock
password=stock123
database=stockdb

mysql -u root -proot123 -e "
CREATE DATABASE IF NOT EXISTS $database DEFAULT CHARACTER SET utf8;
CREATE USER '$username'@'%' IDENTIFIED BY '$password';
CREATE USER '$username'@'localhost' IDENTIFIED BY '$password';
GRANT ALL PRIVILEGES ON $database.* to '$username'@'%';
GRANT ALL PRIVILEGES ON $database.* to '$username'@'localhost';
FLUSH PRIVILEGES;
"
