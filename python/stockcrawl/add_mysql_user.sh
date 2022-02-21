# Resolve host windows IP and then interface with mysql DB installed in windows
username=mdx
password=mdx123
database=mdx

hostip=`cat /etc/resolv.conf | grep nameserver | cut -d ' ' -f 2`
echo $hostip

mysql -h$hostip -u stock -pstock123 -e "
CREATE DATABASE IF NOT EXISTS $database DEFAULT CHARACTER SET utf8;
CREATE USER '$username'@'%' IDENTIFIED BY '$password';
CREATE USER '$username'@'localhost' IDENTIFIED BY '$password';
GRANT ALL PRIVILEGES ON $database.* to '$username'@'%';
GRANT ALL PRIVILEGES ON $database.* to '$username'@'localhost';
FLUSH PRIVILEGES;
"
