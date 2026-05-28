require('dotenv').config();
const { ClobClient } = require('@polymarket/clob-client');
const { ethers } = require('ethers');

async function execute() {
    const args = process.argv.slice(2);
    if (args.length < 4) {
        console.error("Usage: node execute_order.js <tokenId> <price> <side> <size>");
        process.exit(1);
    }

    const [tokenId, price, side, size] = args;
    
    try {
        const privateKey = process.env.PRIVATE_KEY.startsWith('0x') ? process.env.PRIVATE_KEY : '0x' + process.env.PRIVATE_KEY;
        const wallet = new ethers.Wallet(privateKey);
        
        const creds = {
            key: process.env.POLYMARKET_API_KEY,
            secret: process.env.POLYMARKET_SECRET,
            passphrase: process.env.POLYMARKET_PASSPHRASE
        };

        const client = new ClobClient(
            "https://clob.polymarket.com",
            137,
            wallet,
            creds
        );

        // Crear la orden usando el SDK oficial
        const order = await client.createOrder({
            tokenID: tokenId,
            price: parseFloat(price),
            side: side === "YES" || side === "BUY" ? "BUY" : "SELL",
            size: parseFloat(size),
            feeRateBps: 0
        });

        // Enviar la orden
        const resp = await client.postOrder(order);
        
        if (resp && resp.orderID) {
            console.log(JSON.stringify({ ok: true, orderId: resp.orderID }));
        } else {
            console.log(JSON.stringify({ ok: false, error: "Rechazado por el CLOB: " + JSON.stringify(resp) }));
        }
    } catch (e) {
        const errorMsg = e.response && e.response.data ? e.response.data.error || JSON.stringify(e.response.data) : e.message;
        console.log(JSON.stringify({ ok: false, error: errorMsg }));
    }
}

execute();
