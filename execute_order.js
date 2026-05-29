require('dotenv').config();
const { ClobClient } = require('@polymarket/clob-client');
const { ethers } = require('ethers');

async function execute() {
    const args = process.argv.slice(2);
    if (args.length < 4) {
        console.error("Usage: node execute_order.js <conditionId> <price> <side> <size>");
        process.exit(1);
    }

    const [conditionId, price, targetOutcome, size] = args;
    
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

        // 1. Fetch market to get the tokenID for the desired outcome (YES/NO)
        const marketResp = await fetch(`https://clob.polymarket.com/markets/${conditionId}`);
        if (!marketResp.ok) {
            throw new Error(`Failed to fetch market ${conditionId}`);
        }
        const marketData = await marketResp.json();
        
        // Mapear YES -> ['YES', 'UP'] y NO -> ['NO', 'DOWN']
        const validOutcomes = (targetOutcome === "YES" || targetOutcome === "BUY") 
            ? ["YES", "UP"] 
            : ["NO", "DOWN"];
            
        let actualTokenId = null;
        for (const token of marketData.tokens) {
            const outcomeStr = String(token.outcome).toUpperCase();
            if (validOutcomes.includes(outcomeStr)) {
                actualTokenId = token.token_id;
                break;
            }
        }
        
        if (!actualTokenId) {
            throw new Error(`Token ID for outcome ${targetOutcome} not found in market ${conditionId}`);
        }

        // 2. Crear la orden usando el SDK oficial (Siempre compramos el token ganador)
        const feeRateBps = marketData.taker_base_fee || marketData.maker_base_fee || 1000;
        
        const order = await client.createOrder({
            tokenID: actualTokenId,
            price: parseFloat(price),
            side: "BUY",
            size: parseFloat(size),
            feeRateBps: feeRateBps
        });

        // 3. Enviar la orden
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
