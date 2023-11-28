import { InjectQueue, Processor, WorkerHost } from '@nestjs/bullmq';
import { OnModuleInit } from '@nestjs/common';
import { Job, Queue } from 'bullmq';
import { stringify as csvStringify  } from 'csv-stringify/sync';

// import BN from 'bn.js';
// import * as d3 from 'd3-array';
import { OlService } from './ol.service.js';
// import { OlDbService } from '../ol-db/ol-db.service.js';
// import { TransactionView } from './views/TransactionView.js';

import { ClickhouseService } from '../clickhouse/clickhouse.service.js';
import { Block_Metadata_Transactions, Types } from 'aptos';

// const ONE = new BN(1);
// 
// const bnBisect = d3.bisector((a: BN, b: BN) => {
//   if (a.lt(b)) {
//     return -1;
//   }
//   if (a.gt(b)) {
//     return 1;
//   }
//   return 0;
// });
// 
// // Find a BN number in a ascending sorted list
// const bnFindIndex = (list: BN[], element: BN): number => {
//   const index = bnBisect.center(list, element);
//   if (index === -1 || index >= list.length) {
//     return -1;
//   }
//   if (list[index].eq(element)) {
//     return index;
//   }
//   return -1;
// };

export interface VersionJobData {
  version: string;
}

@Processor('ol-version-v7')
export class OlVersionProcessor extends WorkerHost implements OnModuleInit {
  public constructor(
    @InjectQueue('ol-version-v7')
    private readonly olVersionQueue: Queue,

    private readonly olService: OlService,

    // private readonly olDbService: OlDbService,

    private readonly clichouseService: ClickhouseService,
  ) {
    super();
  }

  public async onModuleInit() {
    // await this.olVersionQueue.add('getMissingVersions', undefined, {
    //   repeat: {
    //     every: 10 * 1_000, // 10 seconds
    //   },
    // });

    await this.olVersionQueue.add('fetchLatestVersion', undefined, {
      repeat: {
        every: 10 * 1_000, // 10 seconds
      },
    });
  }

  public async process(job: Job<VersionJobData, any, string>) {
    switch (job.name) {
      // case 'getMissingVersions':
      //   await this.getMissingVersions();
      //   break;

      case 'version':
        await this.processVersion(job.data.version);
        break;

      case 'fetchLatestVersion':
        await this.fetchLatestVersion();
        break;

      default:
        throw new Error(`invalid job name ${job.name}`);
    }
  }

  private async processVersion(version: string) {
    console.log('processVersion', version);

    const transactions = await this.olService.aptosClient.getTransactions({
      start: parseInt(version, 10),
      limit: 1,
    });

    if (!transactions.length) {
      throw new Error(`transaction not found ${version}`);
    }

    await this.ingestTransaction(transactions[0]);
  }

  private async fetchLatestVersion() {
    const ledgerInfo = await this.olService.aptosClient.getLedgerInfo();

    const version = ledgerInfo.ledger_version;

    await this.olVersionQueue.add('version', { version } as VersionJobData, {
      jobId: `__version__${version}`,
    });

    const v = parseInt(version, 10);
    for (let i = 0; i <= v; ++i) {
      await this.olVersionQueue.add('version', { version: `${i}` } as VersionJobData, {
        jobId: `__version__${i}`,
      });
    }
  }

  // private async getMissingVersions() {
  //   const lastBatchIngestedVersion =
  //     await this.olDbService.getLastBatchIngestedVersion();
  //   const ingestedVersions = await this.olDbService.getIngestedVersions(
  //     lastBatchIngestedVersion,
  //   );
  //   const latestVersion = new BN((await this.olService.getMetadata()).version);

  //   const missingVersions: BN[] = [];
  //   for (
  //     let i = lastBatchIngestedVersion.add(ONE);
  //     i.lt(latestVersion);
  //     i = i.add(new BN(ONE))
  //   ) {
  //     const version = i;
  //     if (bnFindIndex(ingestedVersions, version) !== -1) {
  //       continue;
  //     }
  //     missingVersions.push(version);
  //   }

  //   await this.olVersionQueue.addBulk(
  //     missingVersions.map((version) => ({
  //       name: 'version',
  //       data: {
  //         version: version.toString(),
  //       },
  //       opts: {
  //         jobId: `__version__${version}`,
  //       },
  //     })),
  //   );
  // }

  private async ingestEvents(
    version: string,
    timestamp: string,
    events: Types.Event[]
  ) {
    const payload = csvStringify(events.map((event) => {
      const [moduleAddress, moduleName, ...rest] = event.type.split('::');
      const structName = rest.join('::');

      return [
        version,
        timestamp,
        event.guid.creation_number,
        event.guid.account_address.substring(2).padStart(64, '00'),
        event.sequence_number,
        moduleAddress.substring(2).padStart(64, '00'),
        moduleName,
        structName,
        JSON.stringify(event.data),
      ];
    }));

    await this.clichouseService.client.command({
      query: `
        INSERT INTO "event_v7" (
          "version",
          "timestamp",
          "creation_number",
          "account_address",
          "sequence_number",
          "module_address",
          "module_name",
          "struct_name",
          "data"
        )
        SELECT
          "version",
          "timestamp",
          "creation_number",
          reinterpretAsUInt256(reverse(unhex("account_address"))),
          "sequence_number",
          reinterpretAsUInt256(reverse(unhex("module_address"))),
          "module_name",
          "struct_name",
          "data"
        FROM
          format(
            CSV,
            $$
              version UInt64,
              timestamp UInt64,
              creation_number UInt64,
              account_address String,
              sequence_number UInt64,
              module_address String,
              module_name String,
              struct_name String,
              data String
            $$,
            $$${payload}$$
          )
      `,
    });
  }

  private async ingestTransaction(transaction: Types.Transaction) {

    switch (transaction.type) {
      case "genesis_transaction": {
        const genesisTransaction = transaction as Types.Transaction_GenesisTransaction;

        console.log(genesisTransaction);

        await this.ingestEvents(
          genesisTransaction.version,
          '0',
          genesisTransaction.events,
        );

        throw new Error(`Unsupported transaction type ${transaction.type}`);
      } break;

      case "block_metadata_transaction": {
        const blockMetadataTransaction = transaction as Types.Transaction_BlockMetadataTransaction;

        console.log(blockMetadataTransaction);

        await this.ingestEvents(
          blockMetadataTransaction.version,
          blockMetadataTransaction.timestamp,
          blockMetadataTransaction.events,
        );

        throw new Error(`Unsupported transaction type ${transaction.type}`);
      } break;
      
      case "state_checkpoint_transaction": {
        const stateCheckpointTransaction = transaction as Types.Transaction_StateCheckpointTransaction;

        console.log(stateCheckpointTransaction);

        throw new Error(`Unsupported transaction type ${transaction.type}`);
      } break;

      default:
        throw new Error(`Unsupported transaction type ${transaction.type}`);
    }

  //   const queries: string[] = [];

  //   if (transaction.transaction.type === 'user') {
  //     queries.push(`
  //       INSERT INTO "user_transaction" (
  //         "version",
  //         "timestamp_usecs",
  //         "sender",
  //         "sequence_number",
  //         "max_gas_amount",
  //         "gas_unit_price",
  //         "gas_currency",
  //         "module_address",
  //         "module_name",
  //         "function_name",
  //         "arguments",
  //         "vm_status",
  //         "gas_used"
  //       ) VALUES (
  //         ${transaction.version},
  //         ${transaction.timestamp_usecs},
  //         unhex('${transaction.transaction.sender}'),
  //         ${transaction.transaction.sequence_number},
  //         ${transaction.transaction.max_gas_amount},
  //         ${transaction.transaction.gas_unit_price},
  //         '${transaction.transaction.gas_currency}',
  //         unhex('${transaction.transaction.script.module_address ?? ''}'),
  //         '${transaction.transaction.script.module_name ?? ''}',
  //         '${transaction.transaction.script.function_name ?? ''}',
  //         [${
  //           transaction.transaction.script.arguments_bcs
  //             ? transaction.transaction.script.arguments_bcs
  //                 .map((argument) => `unhex('${argument}')`)
  //                 .join(',')
  //             : ''
  //         }],
  //         '${transaction.vm_status.type}',
  //         ${transaction.gas_used}
  //       )
  //     `);
  //   }

  //   const events = transaction.events;

  //   for (const event of events) {
  //     switch (event.data.type) {
  //       case 'newblock':
  //         queries.push(`
  //           INSERT INTO "new_block" (
  //             "version",
  //             "timestamp_usecs",
  //             "round",
  //             "proposer",
  //             "proposed_time",
  //             "gas_used"
  //           ) VALUES (
  //             ${transaction.version},
  //             ${transaction.timestamp_usecs},
  //             ${event.data.round},
  //             unhex('${event.data.proposer}'),
  //             ${event.data.proposed_time},
  //             ${transaction.gas_used}
  //           )
  //         `);
  //         break;

  //       case 'burn':
  //         queries.push(`
  //           INSERT INTO "burn" (
  //             "version",
  //             "timestamp_usecs",
  //             "amount",
  //             "currency",
  //             "preburn_address"
  //           ) VALUES (
  //             ${transaction.version},
  //             ${transaction.timestamp_usecs},
  //             ${event.data.amount.amount},
  //             '${event.data.amount.currency}',
  //             unhex('${event.data.preburn_address}')
  //           )
  //         `);
  //         break;

  //       case 'cancelburn':
  //         // noop
  //         break;

  //       case 'to_xdx_exchange_rate_update':
  //         // noop
  //         break;

  //       case 'preburn':
  //         // noop
  //         break;

  //       case 'receivedpayment':
  //         queries.push(`
  //           INSERT INTO "received_payment" (
  //             "version",
  //             "timestamp_usecs",
  //             "amount",
  //             "currency",
  //             "sender",
  //             "receiver",
  //             "metadata"
  //           ) VALUES (
  //             ${transaction.version},
  //             ${transaction.timestamp_usecs},
  //             ${event.data.amount.amount},
  //             '${event.data.amount.currency}',
  //             unhex('${event.data.sender}'),
  //             unhex('${event.data.receiver}'),
  //             unhex('${event.data.metadata}')
  //           )
  //         `);
  //         break;

  //       case 'sentpayment':
  //         queries.push(`
  //           INSERT INTO "sent_payment" (
  //             "version",
  //             "timestamp_usecs",
  //             "amount",
  //             "currency",
  //             "sender",
  //             "receiver",
  //             "metadata"
  //           ) VALUES (
  //             ${transaction.version},
  //             ${transaction.timestamp_usecs},
  //             ${event.data.amount.amount},
  //             '${event.data.amount.currency}',
  //             unhex('${event.data.sender}'),
  //             unhex('${event.data.receiver}'),
  //             unhex('${event.data.metadata}')
  //           )
  //         `);
  //         break;

  //       case 'admintransaction':
  //         // noop
  //         break;

  //       case 'newepoch':
  //         // noop
  //         break;

  //       case 'receivedmint':
  //         // noop
  //         break;

  //       case 'compliancekeyrotation':
  //         // noop
  //         break;

  //       case 'baseurlrotation':
  //         // noop
  //         break;

  //       case 'createaccount':
  //         queries.push(`
  //           INSERT INTO "create_account" (
  //             "version",
  //             "timestamp_usecs",
  //             "role_id",
  //             "created_address"
  //           ) VALUES (
  //             ${transaction.version},
  //             ${transaction.timestamp_usecs},
  //             ${event.data.role_id},
  //             unhex('${event.data.created_address}')
  //           )
  //         `);
  //         break;

  //       case 'vaspdomain':
  //         // noop
  //         break;

  //       case 'unknown':
  //       default:
  //         // noop
  //         break;
  //     }
  //   }

  //   queries.push(`
  //     INSERT INTO "ingested_versions" ("version") VALUES (${transaction.version})
  //   `);

  //   for (const query of queries) {
  //     await this.clichouseService.exec(query);
  //   }
  }
}