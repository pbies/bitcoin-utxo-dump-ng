# Bitcoin UTXO Dump NG

	https://github.com/pbies/bitcoin-utxo-dump-ng

First clone the repo:
```bash
git clone https://github.com/pbies/bitcoin-utxo-dump-ng.git
```

Change folder:
```bash
cd bitcoin-utxo-dump-ng
```

Then run:
```bash
./01init
./02run
```

If not working - add executable flag to scripts:
```bash
chmod +x ./0*
```

The code is multithreaded (MT) so it will run faster than the original one.

Chainstate folder (`~/.bitcoin/chainstate`) may be corrupted after dump.
Then you need to reindex-chainstate in Bitcoin Core. Or restore chainstate folder from backup.

Original work:

https://github.com/in3rsha/bitcoin-utxo-dump
