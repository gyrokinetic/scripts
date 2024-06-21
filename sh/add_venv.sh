version=$1
env_name=$2

[ $version ] || version=3.12

if [ "$env_name" = "" ]; then
   env_name=ml3
fi

echo "version="$version
echo "env_name="$env_name

sudo apt install -y python${version} python${version}-venv
pv=`which python${version}`
python${version} -m venv $env_name
