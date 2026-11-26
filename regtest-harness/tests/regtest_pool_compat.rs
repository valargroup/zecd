//! Mining and payout RPC contracts exercised against a real node and wallet.
//! Stock-client fee budgets and successful, fully funded payments are separate cases.
//! Lightwalletd has no mining RPC; this suite runs on the Zebra and Zakura node legs.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use zecd_regtest_harness::{
    derive_address_offline, derive_funder_transparent_address, pick_port, resolve_node_bin, Zebrad,
    Zecd, ZecdConfig, COINBASE_MATURITY,
};

/// The pool wallet, restored from the repo's committed throwaway regtest phrase.
const POOL_MNEMONIC: &str = "mechanic vehicle helmet decide plug gorilla frost dial october \
midnight culture idea mountain fame park social drip bid doctor scatter glance defy moment stage";

/// Blocks mined before zecd starts, below the wallet's birthday (see `regtest_coinbase.rs`).
const SEED_BLOCKS: u32 = 2;
const BIRTHDAY: u32 = SEED_BLOCKS + 1;
/// Blocks the pool "finds" once zecd is running.
const POOL_BLOCKS: u32 = 4;
/// Miningcore pays at most this many recipients per `z_sendmany`.
const MINER_PAYOUTS: usize = 50;
/// Miningcore's payout `minconf` and its hard-coded fee.
const PAYOUT_MINCONF: u32 = 8;
const MININGCORE_FEE: f64 = 0.0001;
/// What each miner is paid, in ZEC.
const PAYOUT_ZEC: f64 = 0.01;
/// ZIP 317's marginal fee per logical action, in zatoshis.
const ZIP317_MARGINAL_FEE: u64 = 5_000;

const SYNC_TIMEOUT: Duration = Duration::from_secs(240);
const OP_TIMEOUT: Duration = Duration::from_secs(300);

fn zats(zec: f64) -> u64 {
    (zec.abs() * 1e8).round() as u64
}

async fn node_tip(zebrad: &Zebrad) -> u64 {
    zebrad
        .rpc("getblockcount", json!([]))
        .await
        .expect("node getblockcount")
        .as_u64()
        .expect("block count")
}

/// Poll `getpeerinfo` until zecd's upstream is ready. Call only once a block at or above the
/// wallet's birthday exists.
async fn wait_ready(zecd: &Zecd) {
    let deadline = Instant::now() + SYNC_TIMEOUT;
    loop {
        let peers = zecd
            .call("getpeerinfo", json!([]))
            .await
            .expect("getpeerinfo");
        if peers
            .as_array()
            .and_then(|a| a.first())
            .is_some_and(|p| p["conn_state"] == "ready")
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "zecd never reached ready: {peers}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn mine(zebrad: &Zebrad, zecd: &Zecd, blocks: u32) {
    zebrad.generate_blocks(blocks).await.expect("mine blocks");
    zecd.wait_until_synced_to_node(zebrad, SYNC_TIMEOUT)
        .await
        .expect("zecd sync to the node tip");
}

/// Wait for an async operation the way Miningcore does: poll `z_getoperationresult [[opid]]`
/// until it returns the finished operation, then take its txid.
async fn completed_operation(zecd: &Zecd, opid: &str) -> Value {
    let deadline = Instant::now() + OP_TIMEOUT;
    loop {
        let results = zecd
            .call("z_getoperationresult", json!([[opid]]))
            .await
            .expect("z_getoperationresult");
        if let Some(op) = results.as_array().and_then(|a| a.first()) {
            return op.clone();
        }
        assert!(
            Instant::now() < deadline,
            "operation {opid} did not finish in {OP_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn operation_txid(zecd: &Zecd, opid: &str) -> String {
    let op = completed_operation(zecd, opid).await;
    assert_eq!(op["status"], "success", "{op}");
    op["result"]["txid"]
        .as_str()
        .expect("successful transaction")
        .to_owned()
}

/// Both clients can reserve only 10,000 zatoshis. Check the final wallet operation
/// and the upstream mempool, since receiving an operation ID only acknowledges the request.
async fn assert_fee_shortfall(zecd: &Zecd, node: &Zebrad, params: Value) {
    let before = node.rpc("getrawmempool", json!([])).await.unwrap();
    let opid = zecd.call("z_sendmany", params).await.unwrap();
    let op = completed_operation(zecd, opid.as_str().unwrap()).await;
    assert_eq!(op["status"], "failed", "{op}");
    assert_eq!(op["error"]["code"], -6, "{op}");
    assert!(op["result"]["txid"].is_null());
    assert_eq!(node.rpc("getrawmempool", json!([])).await.unwrap(), before);
}

async fn wire_reply(cfg: &ZecdConfig, request: Value) -> String {
    reqwest::Client::new()
        .post(format!("http://127.0.0.1:{}/", cfg.rpc_port))
        .basic_auth(&cfg.rpc_user, Some(&cfg.rpc_password))
        .json(&request)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap()
}

async fn assert_balance_precision(cfg: &ZecdConfig) -> f64 {
    let wire = wire_reply(cfg, json!({"method":"getbalance","params":[],"id":1})).await;
    // s-nomp reads the digits between the decimal point and the first comma after result.
    let amount = wire
        .split_once("\"result\":")
        .unwrap()
        .1
        .split(',')
        .next()
        .unwrap();
    let decimals = amount.split_once('.').unwrap().1;
    assert!(decimals.bytes().all(|b| b.is_ascii_digit()), "{wire}");
    assert_eq!(10u64.pow(decimals.len() as u32), 100_000_000, "{wire}");
    amount.parse().unwrap()
}

/// The first transaction (the coinbase) of the block at `height`.
async fn coinbase_txid_at(zebrad: &Zebrad, height: u64) -> String {
    let hash = zebrad
        .rpc("getblockhash", json!([height]))
        .await
        .expect("node getblockhash");
    let block = zebrad
        .rpc("getblock", json!([hash, 1]))
        .await
        .expect("node getblock");
    block["tx"][0].as_str().expect("coinbase txid").to_string()
}

/// The category of the first `gettransaction.details` entry, the field both pool packages read
/// to decide whether a block still counts.
async fn coinbase_category(zecd: &Zecd, txid: &str) -> Value {
    let tx = zecd
        .call("gettransaction", json!([txid]))
        .await
        .expect("gettransaction of a coinbase");
    assert_eq!(
        tx["generated"],
        json!(true),
        "a coinbase is generated: {tx}"
    );
    tx["details"][0]["category"].clone()
}

#[tokio::test]
async fn regtest_pool_rpc_contracts() {
    let Some(node_bin) = resolve_node_bin() else {
        assert!(
            std::env::var_os("ZECD_REGTEST_NODE").is_none(),
            "a selected regtest node requires its executable (ZEBRAD_BIN or ZAKURAD_BIN)"
        );
        eprintln!(
            "SKIP regtest_pool_rpc_contracts: set the node binary \
             variable (ZEBRAD_BIN / ZAKURAD_BIN) to run it. The harness still compiled."
        );
        return;
    };

    // The pool's addresses: the coinbase t-address (s-nomp `address`, Miningcore `address`) and
    // the unified address the coinbase is shielded to (s-nomp `zAddress`, Miningcore
    // `z-address`). Workers are transparent addresses this wallet does not own.
    let pool_taddr = derive_address_offline(POOL_MNEMONIC, "transparent", 0)
        .expect("derive the pool t-address offline");
    let pool_ua =
        derive_address_offline(POOL_MNEMONIC, "orchard", 0).expect("derive the pool UA offline");
    let workers: Vec<String> = (10..10 + MINER_PAYOUTS as u32)
        .map(|i| derive_funder_transparent_address(i).expect("derive a worker address"))
        .collect();

    let mut zebrad = Zebrad::start_with_miner(&node_bin, &pool_taddr)
        .await
        .expect("start the node");
    zebrad
        .generate_blocks(SEED_BLOCKS)
        .await
        .expect("seed the chain");

    // No privacy setting: both pool packages pass no policy, so this runs on zecd's defaults.
    // z_shieldcoinbase defaults to AllowRevealedSenders like zcashd, and the default
    // AllowRevealedRecipients lets the UA pay transparent miners.
    let mut cfg = ZecdConfig::new(zebrad.rpc_port, pick_port().expect("pick zecd rpc port"));
    cfg.transparent = true;
    cfg.restore_mnemonic = Some(POOL_MNEMONIC.to_string());
    cfg.birthday = Some(BIRTHDAY);
    let mut zecd = Zecd::start(&cfg).await.expect("start zecd");

    let first_pool_block = node_tip(&zebrad).await + 1;
    zebrad
        .generate_blocks(POOL_BLOCKS)
        .await
        .expect("mine the pool's blocks");
    let last_pool_block = first_pool_block + u64::from(POOL_BLOCKS) - 1;
    wait_ready(&zecd).await;
    zecd.wait_until_synced_to_node(&zebrad, SYNC_TIMEOUT)
        .await
        .expect("zecd sync");

    assert_eq!(assert_balance_precision(&cfg).await, 0.0);
    // 1. Miningcore's startup batch and s-nomp's startup checks.
    let v = zecd
        .call("validateaddress", json!([pool_taddr]))
        .await
        .expect("validateaddress");
    assert_eq!(v["isvalid"], json!(true), "{v}");
    assert_eq!(v["ismine"], true);
    let foreign = zecd
        .call("validateaddress", json!([workers[0]]))
        .await
        .unwrap();
    assert_eq!(foreign["ismine"], false);
    let v = zecd
        .call("z_validateaddress", json!([pool_ua]))
        .await
        .expect("z_validateaddress");
    assert_eq!(v["isvalid"], json!(true), "{v}");
    let v = zecd
        .call("getaddressinfo", json!([pool_taddr]))
        .await
        .expect("getaddressinfo");
    assert_eq!(
        v["ismine"],
        json!(true),
        "the pool address is the wallet's: {v}"
    );
    // Miningcore probes submitblock with no params and expects the help error (-1).
    let e = zecd
        .call("submitblock", json!([]))
        .await
        .expect_err("submitblock with no block is refused by the node");
    assert_eq!(e.code(), Some(-1), "stock mining clients require -1: {e}");
    zecd.call("getblockchaininfo", json!([]))
        .await
        .expect("getblockchaininfo");
    let difficulty = zecd
        .call("getdifficulty", json!([]))
        .await
        .expect("getdifficulty");
    assert_eq!(
        difficulty,
        zebrad
            .rpc("getdifficulty", json!([]))
            .await
            .expect("node getdifficulty"),
        "getdifficulty is the node's answer"
    );
    let subsidy = zecd
        .call("getblocksubsidy", json!([]))
        .await
        .expect("getblocksubsidy");
    assert_eq!(
        subsidy,
        zebrad
            .rpc("getblocksubsidy", json!([]))
            .await
            .expect("node getblocksubsidy"),
        "getblocksubsidy is the node's answer"
    );
    let mining = zecd
        .call("getmininginfo", json!([]))
        .await
        .expect("getmininginfo");
    assert!(mining["difficulty"].is_number(), "{mining}");
    assert_eq!(
        mining["difficulty"],
        zebrad.rpc("getdifficulty", json!([])).await.unwrap()
    );
    zecd.call("getnetworkinfo", json!([]))
        .await
        .expect("getnetworkinfo");
    zecd.call("getnetworkhashps", json!([]))
        .await
        .expect("getnetworkhashps");
    zecd.call("getbalance", json!([]))
        .await
        .expect("getbalance");
    // s-nomp asks for the account of each address and carries on when the call fails.
    assert!(zecd.call("getaccount", json!([pool_taddr])).await.is_err());

    // 2. Forwarded calls give the node's answers.
    let template_request =
        json!([{ "capabilities": ["coinbasetxn", "workid", "coinbase/append"] }]);
    let via_node = zebrad
        .rpc("getblocktemplate", template_request.clone())
        .await
        .expect("node getblocktemplate");
    let via_zecd = zecd
        .call("getblocktemplate", template_request)
        .await
        .expect("getblocktemplate through zecd");
    for field in ["height", "previousblockhash"] {
        assert_eq!(
            via_zecd[field], via_node[field],
            "getblocktemplate {field} matches the node"
        );
    }
    let tip_hash = zebrad
        .rpc("getbestblockhash", json!([]))
        .await
        .expect("node getbestblockhash");
    let block = zecd
        .call("getblock", json!([tip_hash, 1]))
        .await
        .expect("getblock through zecd");
    assert_eq!(
        block,
        zebrad
            .rpc("getblock", json!([tip_hash, 1]))
            .await
            .expect("node getblock"),
        "getblock is the node's answer"
    );
    // s-nomp's form, with the default verbosity.
    zecd.call("getblock", json!([tip_hash]))
        .await
        .expect("getblock with the default verbosity");
    let raw_tip = zebrad
        .rpc("getblock", json!([tip_hash, 0]))
        .await
        .expect("node raw block");
    let submitted = zecd
        .call("submitblock", json!([raw_tip]))
        .await
        .expect("submitblock through zecd");
    assert_eq!(submitted, json!("duplicate"), "the tip is already known");

    // Build on the same parent on an isolated producer. Only zecd submits the
    // fresh block to the wallet's node. Miningcore immediately looks it up in the batch.
    let producer = Zebrad::start_with_miner(&node_bin, &pool_taddr)
        .await
        .unwrap();
    for height in 1..=last_pool_block {
        let hash = zebrad.rpc("getblockhash", json!([height])).await.unwrap();
        let raw = zebrad.rpc("getblock", json!([hash, 0])).await.unwrap();
        assert!(producer
            .rpc("submitblock", json!([raw]))
            .await
            .unwrap()
            .is_null());
    }
    producer.generate_blocks(1).await.unwrap();
    let fresh = producer.rpc("getbestblockhash", json!([])).await.unwrap();
    let raw = producer.rpc("getblock", json!([fresh, 0])).await.unwrap();
    assert!(zebrad.rpc("getblock", json!([fresh])).await.is_err());
    let batch: Value = serde_json::from_str(
        &wire_reply(
            &cfg,
            json!([
                {"id":1,"method":"submitblock","params":[raw]},
                {"id":2,"method":"getblock","params":[fresh]}
            ]),
        )
        .await,
    )
    .unwrap();
    assert!(
        batch[0]["error"].is_null() && batch[0]["result"].is_null(),
        "{batch}"
    );
    assert_eq!(batch[0]["id"], 1);
    assert_eq!(batch[1]["id"], 2);
    assert_eq!(batch[1]["result"]["hash"], fresh);
    assert_eq!(
        zebrad.rpc("getbestblockhash", json!([])).await.unwrap(),
        fresh
    );
    assert_eq!(
        zecd.call("submitblock", json!([raw])).await.unwrap(),
        "duplicate"
    );
    drop(producer);

    // 3. The coinbase ladder: immature until it can be spent, then generate.
    let first_coinbase = coinbase_txid_at(&zebrad, first_pool_block).await;
    assert_eq!(
        coinbase_category(&zecd, &first_coinbase).await,
        json!("immature")
    );
    let tip = node_tip(&zebrad).await;
    let at_99 = first_pool_block + u64::from(COINBASE_MATURITY) - 2;
    mine(&zebrad, &zecd, (at_99 - tip) as u32).await;
    assert_eq!(coinbase_category(&zecd, &first_coinbase).await, "immature");
    mine(&zebrad, &zecd, 1).await;
    assert_eq!(coinbase_category(&zecd, &first_coinbase).await, "generate");
    let tip = node_tip(&zebrad).await;
    // Mature every pool block: a coinbase spends at COINBASE_MATURITY confirmations.
    let target = last_pool_block + u64::from(COINBASE_MATURITY) - 1;
    mine(&zebrad, &zecd, (target - tip) as u32).await;
    assert_eq!(
        coinbase_category(&zecd, &first_coinbase).await,
        json!("generate")
    );
    // s-nomp's listing: mature coinbase at the pool address, marked generated.
    let unspent = zecd
        .call("listunspent", json!([1, 99_999_999, [pool_taddr]]))
        .await
        .expect("listunspent");
    let entries = unspent.as_array().expect("listunspent array");
    assert!(
        entries.iter().any(|e| e["txid"] == json!(first_coinbase)),
        "the mature coinbase is listed: {unspent}"
    );
    assert!(
        entries.iter().all(|e| e["generated"] == json!(true)),
        "coinbase outputs are generated: {unspent}"
    );
    // z_getbalance on the t-address is what listunspent lists there.
    let listed: u64 = entries
        .iter()
        .map(|e| zats(e["amount"].as_f64().expect("amount")))
        .sum();
    let t_balance = zecd
        .call("z_getbalance", json!([pool_taddr, 1]))
        .await
        .expect("z_getbalance t-address");
    assert_eq!(zats(t_balance.as_f64().expect("number")), listed);

    // 4. Shield the coinbase into the UA, in the two-argument form both packages send.
    let shield = zecd
        .call("z_shieldcoinbase", json!([pool_taddr, pool_ua]))
        .await
        .expect("z_shieldcoinbase");
    let opid = shield["opid"].as_str().expect("opid").to_string();
    let statuses = zecd
        .call("z_getoperationstatus", Value::Null)
        .await
        .expect("z_getoperationstatus with null params (s-nomp)");
    assert!(
        statuses
            .as_array()
            .is_some_and(|a| a.iter().any(|s| s["id"] == json!(opid))),
        "{statuses}"
    );
    let shield_txid = operation_txid(&zecd, &opid).await;
    let raw = zebrad
        .rpc("getrawtransaction", json!([shield_txid, 1]))
        .await
        .expect("node getrawtransaction");
    assert_eq!(
        raw["vout"].as_array().map(Vec::len),
        Some(0),
        "a coinbase shielding has no transparent outputs: {raw}"
    );
    // zecd's `minconf` N treats a shielded note as spendable one block later than zcashd does:
    // `listunspent` reports N confirmations after N blocks, but `getbalance`, `z_getbalance`
    // and `z_sendmany` at minconf N only count it after N + 1. The balance and the send agree,
    // which is what Miningcore relies on, so mine the extra block rather than test the offset.
    mine(&zebrad, &zecd, PAYOUT_MINCONF + 1).await;

    // 5. The shielded balance, at Miningcore's minconf, is exactly the shielded notes, which
    //    are Ironwood notes on this NU6.3-active chain.
    let unspent = zecd
        .call("listunspent", json!([PAYOUT_MINCONF]))
        .await
        .expect("listunspent");
    let shielded: Vec<&Value> = unspent
        .as_array()
        .expect("array")
        .iter()
        .filter(|e| e["txid"] == json!(shield_txid))
        .collect();
    assert!(
        !shielded.is_empty(),
        "the shielding output is listed: {unspent}"
    );
    assert!(
        shielded.iter().all(|e| e["pool"] == "ironwood"),
        "the coinbase is shielded into Ironwood: {unspent}"
    );
    let shielded_total: u64 = shielded
        .iter()
        .map(|e| zats(e["amount"].as_f64().expect("amount")))
        .sum();
    let z_balance = zecd
        .call("z_getbalance", json!([pool_ua, PAYOUT_MINCONF]))
        .await
        .expect("z_getbalance UA");
    assert_eq!(
        zats(z_balance.as_f64().expect("number")),
        shielded_total,
        "z_getbalance on the UA is the shielded coinbase"
    );

    // 6. Miningcore's payout: 50 transparent recipients, four positional args, a fixed fee.
    let recipients: Vec<Value> = workers
        .iter()
        .map(|w| json!({ "address": w, "amount": PAYOUT_ZEC }))
        .collect();
    let mut near_balance = recipients.clone();
    let budget = zats(z_balance.as_f64().unwrap()) - zats(MININGCORE_FEE);
    let other_payments = (MINER_PAYOUTS as u64 - 1) * zats(PAYOUT_ZEC);
    near_balance[0]["amount"] = json!((budget - other_payments) as f64 / 1e8);
    assert_fee_shortfall(
        &zecd,
        &zebrad,
        json!([pool_ua, near_balance, PAYOUT_MINCONF, MININGCORE_FEE]),
    )
    .await;
    let opid = zecd
        .call(
            "z_sendmany",
            json!([pool_ua, recipients, PAYOUT_MINCONF, MININGCORE_FEE]),
        )
        .await
        .expect("Miningcore z_sendmany");
    let payout_txid = operation_txid(&zecd, opid.as_str().expect("opid")).await;
    mine(&zebrad, &zecd, 1).await;
    let payout = zecd
        .call("gettransaction", json!([payout_txid]))
        .await
        .expect("gettransaction of the payout");
    let fee = zats(payout["fee"].as_f64().expect("payout fee"));
    assert!(
        fee > zats(MININGCORE_FEE)
            && fee >= MINER_PAYOUTS as u64 * ZIP317_MARGINAL_FEE
            && fee.is_multiple_of(ZIP317_MARGINAL_FEE),
        "the ZIP 317 fee is paid, not the requested 0.0001: {fee} zatoshis"
    );
    let paid: BTreeSet<String> = payout["details"]
        .as_array()
        .expect("details")
        .iter()
        .filter(|d| {
            d["category"] == "send" && zats(d["amount"].as_f64().unwrap_or(0.0)) == zats(PAYOUT_ZEC)
        })
        .filter_map(|d| d["address"].as_str().map(str::to_string))
        .collect();
    let expected: BTreeSet<String> = workers.iter().cloned().collect();
    assert_eq!(paid, expected, "every miner is paid once: {payout}");
    let raw = zebrad
        .rpc("getrawtransaction", json!([payout_txid, 1]))
        .await
        .expect("node getrawtransaction");
    assert_eq!(
        raw["vout"].as_array().map(Vec::len),
        Some(MINER_PAYOUTS),
        "only the miners' outputs are transparent: {raw}"
    );
    mine(&zebrad, &zecd, 10).await;

    // 7. s-nomp's refill of its transparent payout address: two-argument z_sendmany.
    let t_payout = zecd
        .call("getnewaddress", json!(["", "transparent"]))
        .await
        .expect("getnewaddress transparent")
        .as_str()
        .expect("address")
        .to_string();
    assert_eq!(
        zecd.call("validateaddress", json!([t_payout]))
            .await
            .unwrap()["ismine"],
        true
    );
    let balance = zecd
        .call("z_getbalance", json!([pool_ua, 1]))
        .await
        .unwrap();
    let sweep = (zats(balance.as_f64().unwrap()) - 10_000) as f64 / 1e8;
    assert_fee_shortfall(
        &zecd,
        &zebrad,
        json!([pool_ua, [{"address":t_payout,"amount":sweep}]]),
    )
    .await;
    // A client that leaves enough for the transaction can refill the same address.
    let opid = zecd
        .call(
            "z_sendmany",
            json!([pool_ua, [{ "address": t_payout, "amount": 0.5 }]]),
        )
        .await
        .expect("s-nomp z_sendmany");
    let refill_txid = operation_txid(&zecd, opid.as_str().expect("opid")).await;
    mine(&zebrad, &zecd, 10).await;

    let refill_outputs = zecd
        .call("listunspent", json!([1, 9_999_999, [t_payout]]))
        .await
        .unwrap();
    let refill_vout = refill_outputs
        .as_array()
        .unwrap()
        .iter()
        .find(|u| u["txid"] == refill_txid)
        .and_then(|u| u["vout"].as_u64())
        .expect("the confirmed refill pays the payout address");

    // The default policy pays the transparent workers from shielded notes.
    let mut amounts = serde_json::Map::new();
    for w in workers.iter().take(3) {
        amounts.insert(w.clone(), json!(PAYOUT_ZEC));
    }
    let txid = zecd
        .call("sendmany", json!(["", amounts]))
        .await
        .expect("s-nomp sendmany");
    let txid = txid.as_str().expect("sendmany txid").to_string();
    mine(&zebrad, &zecd, 1).await;
    let raw = zebrad
        .rpc("getrawtransaction", json!([txid, 1]))
        .await
        .expect("node getrawtransaction");
    assert!(
        raw["vin"].as_array().unwrap().is_empty(),
        "the default payout uses shielded inputs: {raw}"
    );
    let sent = zecd
        .call("gettransaction", json!([txid]))
        .await
        .expect("gettransaction of the s-nomp payout");
    let paid: BTreeSet<&str> = sent["details"]
        .as_array()
        .expect("details")
        .iter()
        .filter(|d| d["category"] == "send")
        .filter_map(|d| d["address"].as_str())
        .collect();
    for w in workers.iter().take(3) {
        assert!(paid.contains(w.as_str()), "{w} is paid: {sent}");
    }
    assert_ne!(assert_balance_precision(&cfg).await.fract(), 0.0);

    // 9. s-nomp under the policy that lets `sendmany` spend transparent funds. Its balance check
    //    sums `listunspent`, which lists transparent and shielded funds alike, so the payout must
    //    succeed from either: from the refilled t-address while it covers the payout, and from
    //    shielded notes once it does not, rather than failing with -6.
    cfg.privacy_policy = Some("AllowFullyTransparent".to_string());
    cfg.pipeline_proving = Some(true);
    zecd.restart(&cfg)
        .await
        .expect("restart zecd with the transparent policy");
    zecd.wait_until_synced_to_node(&zebrad, SYNC_TIMEOUT)
        .await
        .expect("zecd sync");
    // The first payout spends shielded notes. Submit the transparent payout while the first
    // operation is active: both have their own funds, but the wallet must commit them in order.
    mine(&zebrad, &zecd, 3).await;
    let full_balance = zecd
        .call("z_getbalance", json!([pool_ua, 1]))
        .await
        .unwrap();
    let op = zecd.call("z_sendmany", json!([
        pool_ua, [{"address": workers[5], "amount": PAYOUT_ZEC}], 1, null, "AllowRevealedRecipients"
    ])).await.expect("start shielded payout");
    let opid = op.as_str().unwrap();
    let deadline = Instant::now() + OP_TIMEOUT;
    loop {
        let status = zecd
            .call("z_getoperationstatus", json!([[opid]]))
            .await
            .unwrap();
        match status[0]["status"].as_str() {
            Some("executing" | "success") => break,
            Some("queued") if Instant::now() < deadline => tokio::task::yield_now().await,
            _ => panic!("shielded payout did not start: {status}"),
        }
    }
    let small: serde_json::Map<String, Value> = [(workers[3].clone(), json!(PAYOUT_ZEC))]
        .into_iter()
        .collect();
    // One request fits in the transparent refill. The other must try the shielded fallback,
    // where even the pre-payout balance leaves nothing for the fee.
    let (from_t, shortfall) = tokio::join!(
        zecd.call("sendmany", json!(["", small])),
        zecd.call(
            "sendmany",
            json!(["", { (workers[4].as_str()): full_balance }])
        ),
    );
    let from_t = from_t.expect("sendmany covered by transparent funds");
    let shortfall = shortfall.expect_err("the full balance cannot cover both payout and fee");
    assert_eq!(shortfall.code(), Some(-6), "{shortfall}");
    assert!(
        shortfall.to_string().contains("insufficient funds"),
        "{shortfall}"
    );
    let mempool = zebrad.rpc("getrawmempool", json!([])).await.unwrap();
    let first_txid = operation_txid(&zecd, opid).await;
    assert!(
        mempool
            .as_array()
            .unwrap()
            .iter()
            .any(|txid| txid == &first_txid),
        "the earlier shielded payout must be broadcast before the queued payouts finish"
    );
    mine(&zebrad, &zecd, 1).await;
    let first = zebrad
        .rpc("getrawtransaction", json!([first_txid, 1]))
        .await
        .unwrap();
    assert_eq!(
        first["vin"].as_array().map(Vec::len),
        Some(0),
        "the named shielded source must stay shielded: {first}"
    );
    let raw = zebrad
        .rpc("getrawtransaction", json!([from_t, 1]))
        .await
        .expect("node getrawtransaction");
    assert!(
        raw["vin"]
            .as_array()
            .expect("vin")
            .iter()
            .any(|input| input["txid"] == refill_txid && input["vout"] == refill_vout),
        "the transparent payout spends the exact refill output: {raw}"
    );
    mine(&zebrad, &zecd, 10).await;

    // Everything a transparent send may spend: the refilled address and the transparent change
    // of the payout above, but not coinbase, which only `z_shieldcoinbase` may spend.
    let t_left: f64 = zecd
        .call("listunspent", json!([1, 9_999_999]))
        .await
        .expect("listunspent")
        .as_array()
        .expect("unspent list")
        .iter()
        .filter(|u| u["pool"] == "transparent" && u["generated"] != true)
        .filter_map(|u| u["amount"].as_f64())
        .sum();
    let large = ((zats(t_left) + zats(0.1)) as f64) / 1e8;
    let shielded = zecd
        .call("z_getbalance", json!([pool_ua, 1]))
        .await
        .unwrap()
        .as_f64()
        .unwrap();
    assert!(
        shielded > large + 0.01,
        "the shielded notes cover a payout the t-address cannot: {shielded} vs {large}"
    );
    let large_amounts: serde_json::Map<String, Value> =
        [(workers[4].clone(), json!(large))].into_iter().collect();
    let from_z = zecd
        .call("sendmany", json!(["", large_amounts]))
        .await
        .expect("sendmany falls back to shielded notes");
    mine(&zebrad, &zecd, 1).await;
    let raw = zebrad
        .rpc("getrawtransaction", json!([from_z, 1]))
        .await
        .expect("node getrawtransaction");
    assert_eq!(
        raw["vin"].as_array().map(Vec::len),
        Some(0),
        "a payout the t-address cannot cover is paid from shielded notes: {raw}"
    );

    // Create recent rewind checkpoints. Detach a reward and check the same record
    // before and after restarting the wallet on the replacement chain.
    for _ in 0..5 {
        mine(&zebrad, &zecd, 1).await;
    }
    let old_height = node_tip(&zebrad).await;
    let old_hash = zebrad.rpc("getbestblockhash", json!([])).await.unwrap();
    let reward = coinbase_txid_at(&zebrad, old_height).await;
    assert_eq!(coinbase_category(&zecd, &reward).await, "immature");
    zebrad
        .restart_with_miner(zecd_regtest_harness::ALT_MINER_ADDRESS)
        .await
        .unwrap();
    if zebrad
        .rpc("getblockhash", json!([old_height]))
        .await
        .ok()
        .as_ref()
        == Some(&old_hash)
    {
        zebrad
            .rpc("invalidateblock", json!([old_hash]))
            .await
            .unwrap();
    }
    zebrad
        .generate_blocks((old_height + 4 - node_tip(&zebrad).await) as u32)
        .await
        .unwrap();
    zecd.wait_until_synced_to_node(&zebrad, SYNC_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(coinbase_category(&zecd, &reward).await, "orphan");
    zecd.restart(&cfg).await.unwrap();
    zecd.wait_until_synced_to_node(&zebrad, SYNC_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(coinbase_category(&zecd, &reward).await, "orphan");
    let history = zecd
        .call("listtransactions", json!(["*", 1000]))
        .await
        .unwrap();
    assert!(history
        .as_array()
        .unwrap()
        .iter()
        .any(|row| row["txid"] == reward
            && row["generated"] == true
            && row["category"] == "orphan"));
}
