import json

PANCAKESWAP_PREDICTION_ABI = json.loads('''[
    {"inputs":[],"name":"currentEpoch","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
    {"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"rounds","outputs":[
        {"internalType":"uint256","name":"epoch","type":"uint256"},
        {"internalType":"uint256","name":"startTimestamp","type":"uint256"},
        {"internalType":"uint256","name":"lockTimestamp","type":"uint256"},
        {"internalType":"uint256","name":"closeTimestamp","type":"uint256"},
        {"internalType":"int256","name":"lockPrice","type":"int256"},
        {"internalType":"int256","name":"closePrice","type":"int256"},
        {"internalType":"uint256","name":"lockOracleId","type":"uint256"},
        {"internalType":"uint256","name":"closeOracleId","type":"uint256"},
        {"internalType":"uint256","name":"totalAmount","type":"uint256"},
        {"internalType":"uint256","name":"bullAmount","type":"uint256"},
        {"internalType":"uint256","name":"bearAmount","type":"uint256"},
        {"internalType":"uint256","name":"rewardBaseCalAmount","type":"uint256"},
        {"internalType":"uint256","name":"rewardAmount","type":"uint256"},
        {"internalType":"bool","name":"oracleCalled","type":"bool"}
    ],"stateMutability":"view","type":"function"},
    {"inputs":[{"internalType":"uint256","name":"epoch","type":"uint256"}],"name":"betBull","outputs":[],"stateMutability":"payable","type":"function"},
    {"inputs":[{"internalType":"uint256","name":"epoch","type":"uint256"}],"name":"betBear","outputs":[],"stateMutability":"payable","type":"function"}
]''')
