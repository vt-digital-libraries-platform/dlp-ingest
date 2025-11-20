# valid args: mint, update <noid>
command=$1
noid=$2
cwd=$(pwd)
cd ./src

FULL_URL="https://digital.lib.vt.edu/" \
SHORT_URL="https://idn.lib.vt.edu/" \
API_KEY=eSIaSK2L3y3hWpmSAzPpWaAgnghnRrEVabe5KbR4  \
API_ENDPOINT="https://2xmdyl893j.execute-api.us-east-1.amazonaws.com/Prod/" \
python3 NoidTools.py $command $noid
cd $cwd