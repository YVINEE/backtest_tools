const ccxt = require('ccxt');
const fs = require('fs')
const createCsvWriter = require('csv-writer').createArrayCsvWriter;
const exchange_limit = JSON.parse(fs.readFileSync(`${__dirname}/exchange_limit.json`, 'utf8'));
const tf_ms = JSON.parse(fs.readFileSync(`${__dirname}/tf_ms.json`, 'utf8'));

function date_to_timestamp(my_date) {
    my_date = my_date.split("-");
    let newDate = new Date(Date.UTC(my_date[2], my_date[1] - 1, my_date[0]));
    return newDate.getTime();
}

function timestamp_to_date(my_tf) {
    my_date = new Date(my_tf);
    str_date = `${my_date.getUTCDate()}-${my_date.getUTCMonth()+1}-${my_date.getUTCFullYear()} ${my_date.getUTCHours()}:${my_date.getUTCMinutes()}`
    return str_date;
}

function current_utc_date() {
    const now = new Date();
    now.toUTCString();
    now.toISOString();
    return Math.floor(now);
}

function eliminate_double_ts(arr) {
    let i,
        len = arr.length
    to_remove = []

    for (i = 1; i < len; i++) {
        if (arr[i][0] === arr[i - 1][0]) {
            to_remove.push(i)
        }
    }
    for (i = to_remove.length - 1; i >= 0; i--) {
        arr.splice(to_remove[i], 1);
    }
    return arr;
}

async function get_ohlcv(exchange, pair_name, timeframe, since_date, limit, tf_ms) {
    let exchange_name = exchange.name;
    let starting_date = date_to_timestamp(since_date);
    let now = current_utc_date();
    let tf_array = [starting_date];
    let last_tf = starting_date;
    let result_ohlcv = [];
    let current_request = 0;
    while (last_tf < now) {
        last_tf = last_tf + (limit) * tf_ms;
        if (last_tf < now) {
            tf_array.push(last_tf);
        }
    }
    let total_request = tf_array.length;

    for (const tf in tf_array) {
        exchange.fetchOHLCV(symbol = pair_name, timeframe = timeframe, since = tf_array[tf], limit = limit).then(resp => {
            result_ohlcv = result_ohlcv.concat(resp);
            current_request++;
        }).catch(err => {
            console.log("Error retrieving candles since", tf_array[tf], exchange_name, pair_name, timeframe);
            exchange.fetchOHLCV(symbol = pair_name, timeframe = timeframe, since = tf_array[tf], limit = limit).then(resp => {
                result_ohlcv = result_ohlcv.concat(resp);
                current_request++;
            }).catch(err2 => {
                console.log("Error retrieving candles since", tf_array[tf], exchange_name, pair_name, timeframe);
                exchange.fetchOHLCV(symbol = pair_name, timeframe = timeframe, since = tf_array[tf], limit = limit).then(resp => {
                    result_ohlcv = result_ohlcv.concat(resp);
                    current_request++;
                }).catch(err3 => {
                    console.log(err2);
                    console.log("/! Fatal Error /!", pair_name, timeframe);
                    current_request++;
                })
            })
        })
    }
    const delay = millis => new Promise((resolve, reject) => {
        setTimeout(_ => resolve(), millis);
    });
    while (current_request < total_request) {
        await delay(2000);
    }

    result_ohlcv = result_ohlcv.sort(function(a, b) {
        return a[0] - b[0];
    });

    if (result_ohlcv.length > 0) {
        result_ohlcv = eliminate_double_ts(result_ohlcv);
        let file_pair = pair_name.replace('/', '-');
        let dirpath = `${__dirname}/` + exchange_name.toLowerCase() + '/' + timeframe + '/';
        let filepath = dirpath + file_pair + ".csv";
        let first_date = timestamp_to_date(result_ohlcv[0][0]);
    
        await fs.promises.mkdir(dirpath, { recursive: true });
    
        const csvWriter = createCsvWriter({
            header: ['date', 'open', 'high', 'low', 'close', 'volume'],
            path: filepath
        });
    
        csvWriter.writeRecords(result_ohlcv) // returns a promise
            .then(() => {
                process.stdout.write(`Successfully downloaded ${result_ohlcv.length} candles since ${first_date} in ${filepath}`);
                return true;
            }).catch(err => {
                console.log(err);
                return false;
            });        
    }
    else
    {
        process.stdout.write(`No candles found`);
    }      
}

async function get_multi_ohlcv(exchange, pair_list, tf_list, start_date, exchange_limit_json, tf_ms_json) {
    for (const tf in tf_list) {
        for (const pair in pair_list) {
            await get_ohlcv(
                exchange,
                pair_list[pair],
                tf_list[tf],
                start_date,
                exchange_limit_json[exchange.name],
                tf_ms_json[tf_list[tf]]
            );
        }
    }
}

// --- Edit exchange here ---
let exchange = new ccxt.bitget({ enableRateLimit: true })

// --- Edit coin list here ---
pair = process.argv[2] //['SOL/USDT']

// --- Edit timeframe list and start date here ---
timeframe = ['30m']
start_date =  new Date();
start_date.setDate(start_date.getDate() - 7);
day = start_date.getDate();
month = start_date.getMonth() + 1;
year = start_date.getFullYear();

formated_start_date = `${day}-${month}-${year}`;

get_ohlcv(
	exchange,
	pair,
	timeframe,
	formated_start_date,
	exchange_limit[exchange.name],
	tf_ms[timeframe]
	);