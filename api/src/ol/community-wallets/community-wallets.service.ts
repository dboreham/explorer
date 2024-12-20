import { Injectable } from '@nestjs/common';
import _ from 'lodash';
import { ConfigService } from '@nestjs/config';

import { redisClient } from '../../redis/redis.service.js';
import { OlService } from '../ol.service.js';
import {
  CommunityWallet,
  CommunityWalletInfo,
  CommunityWalletStats,
  CommunityWalletDetails,
  CommunityWalletPayments,
} from './community-wallet.model.js';
import { parseAddress, parseHexString } from '../../utils.js';
import { ICommunityWalletsService } from './interfaces.js';
import {
  COMMUNITY_WALLETS_CACHE_KEY,
  COMMUNITY_WALLETS_STATS_CACHE_KEY,
  COMMUNITY_WALLETS_PAYMENTS_CACHE_KEY,
  COMMUNITY_WALLETS_DETAILS_CACHE_KEY,
} from '../constants.js';
import { OlConfig } from '../../config/config.interface.js';
import axios from 'axios';

function formatCoin(value: number): number {
  return Math.floor(value / 1e6);
}

@Injectable()
export class CommunityWalletsService implements ICommunityWalletsService {
  private readonly cacheEnabled: boolean;
  private readonly communityWalletsUrl?: string;

  public constructor(
    private readonly olService: OlService,
    config: ConfigService,
  ) {
    this.cacheEnabled = config.get<boolean>('cacheEnabled')!;
    this.communityWalletsUrl = config.get<OlConfig>('ol')?.communityWalletsUrl;
  }

  private async getFromCache<T>(key: string): Promise<T | null> {
    const cachedData = await redisClient.get(key);
    if (cachedData) {
      return JSON.parse(cachedData) as T;
    }
    return null;
  }

  private async setCache<T>(key: string, data: T): Promise<void> {
    await redisClient.set(key, JSON.stringify(data));
  }

  public async getCommunityWallets(): Promise<CommunityWallet[]> {
    if (this.cacheEnabled) {
      const cachedWallets = await this.getFromCache<CommunityWallet[]>(COMMUNITY_WALLETS_CACHE_KEY);
      if (cachedWallets) {
        return cachedWallets;
      }
    }

    const wallets = await this.queryCommunityWallets();
    await this.setCache(COMMUNITY_WALLETS_CACHE_KEY, wallets);

    return wallets;
  }

  private async queryCommunityWallets(): Promise<CommunityWallet[]> {
    const communityWallets = await this.loadCommunityWallets();
    const donorVoiceRegistry = (await this.olService.aptosClient.getAccountResource(
      '0x1',
      '0x1::donor_voice::Registry',
    )) as {
      type: '0x1::donor_voice::Registry';
      data: {
        liquidation_queue: [];
        list: string[];
      };
    };

    const addresses = donorVoiceRegistry.data.list.map((address) =>
      parseAddress(address).toString('hex').toUpperCase(),
    );

    const res = await Promise.all(
      addresses.map(async (address) => {
        const addrBuff = parseAddress(address);
        const addr = addrBuff.toString('hex').toUpperCase();
        const info = communityWallets.get(addr);
        const balance = await this.olService.getAccountBalance(addrBuff);

        return {
          address: addr,
          name: info?.name,
          description: info?.description,
          balance: balance ? balance.toNumber() : 0,
        };
      }),
    );

    const sortedRes = _.sortBy(res, [(wallet) => -wallet.balance]);

    return sortedRes.map(
      (wallet, index) =>
        new CommunityWallet({
          rank: index + 1,
          ...wallet,
        }),
    );
  }

  public async getCommunityWalletsStats(): Promise<CommunityWalletStats> {
    if (this.cacheEnabled) {
      const cachedStats = await this.getFromCache<CommunityWalletStats>(
        COMMUNITY_WALLETS_STATS_CACHE_KEY,
      );
      if (cachedStats) {
        return cachedStats;
      }
    }

    const stats = await this.queryCommunityWalletsStats();
    await this.setCache(COMMUNITY_WALLETS_STATS_CACHE_KEY, stats);

    return stats;
  }

  private async queryCommunityWalletsStats(): Promise<CommunityWalletStats> {
    const wallets = await this.queryCommunityWallets();
    const sumBalance = wallets.reduce((acc, wallet) => acc + wallet.balance, 0);
    const totalBalance = Math.floor(sumBalance);

    const { totalPaid, totalVetoed, totalPending } = await this.queryPayments(wallets);

    return new CommunityWalletStats({
      totalBalance,
      totalPaid: formatCoin(totalPaid),
      totalPending: formatCoin(totalPending),
      totalVetoed: formatCoin(totalVetoed),
    });
  }

  async loadCommunityWallets(): Promise<Map<string, CommunityWalletInfo>> {
    const cwsMap = new Map<string, CommunityWalletInfo>();
    if (!this.communityWalletsUrl) {
      return cwsMap;
    }
    try {
      const response = await axios.get(this.communityWalletsUrl);
      const data = response.data;
      Object.keys(data.communityWallets).forEach((address) => {
        let addressStr = address.replace(/^0x/, '').toUpperCase();
        cwsMap.set(addressStr, data.communityWallets[address] as CommunityWalletInfo);
      });
    } catch (error) {
      console.error('Error loading validator handles from URL:', error);
      return new Map<string, CommunityWalletInfo>();
    }
    return cwsMap;
  }

  private async queryPayments(wallets: CommunityWallet[]) {
    let totalPaid = 0;
    let totalVetoed = 0;
    let totalPending = 0;

    const currentEpoch: number = await this.olService.aptosClient
      .getLedgerInfo()
      .then((info) => Number(info.epoch));

    await Promise.all(
      wallets.map(async (wallet) => {
        const resource = await this.queryTransactions(wallet.address);

        resource.data['paid'].forEach((payment) => {
          totalPaid += Number(payment['tx']['value']);
        });

        resource.data['scheduled'].forEach((payment) => {
          if (payment.deadline >= currentEpoch) {
            totalPending += Number(payment['tx']['value']);
          }
        });

        resource.data['veto'].forEach((payment) => {
          totalVetoed += Number(payment['tx']['value']);
        });
      }),
    );

    return { totalPaid, totalVetoed, totalPending };
  }

  private async queryTransactions(address: string) {
    const resource = await this.olService.aptosClient.getAccountResource(
      '0x' + address,
      '0x1::donor_voice_txs::TxSchedule',
    );

    return resource;
  }

  public async getCommunityWalletsPayments(): Promise<CommunityWalletPayments[]> {
    if (this.cacheEnabled) {
      const cachedPayments = await this.getFromCache<CommunityWalletPayments[]>(
        COMMUNITY_WALLETS_PAYMENTS_CACHE_KEY,
      );
      if (cachedPayments) {
        return cachedPayments;
      }
    }

    const payments = await this.queryCommunityWalletsPayments();
    await this.setCache(COMMUNITY_WALLETS_PAYMENTS_CACHE_KEY, payments);

    return payments;
  }

  private async queryCommunityWalletsPayments(): Promise<CommunityWalletPayments[]> {
    const wallets = await this.queryCommunityWallets();
    const currentEpoch = await this.olService.aptosClient
      .getLedgerInfo()
      .then((info) => Number(info.epoch));

    const processPayments = (
      payments: any[],
      status: string,
      currentEpoch: number,
    ): {
      deadline: number;
      payee: string;
      value: number;
      description: string;
      status: string;
    }[] => {
      return payments
        .filter((payment) => status !== 'pending' || payment.deadline >= currentEpoch)
        .map((payment) => ({
          deadline: Number(payment.deadline),
          payee: payment.tx.payee,
          value: formatCoin(payment.tx.value),
          description: Buffer.from(parseHexString(payment.tx.description)).toString('utf-8'),
          status,
        }));
    };

    return Promise.all(
      wallets.map(async (wallet) => {
        try {
          const resource = await this.olService.aptosClient.getAccountResource(
            '0x' + wallet.address,
            '0x1::donor_voice_txs::TxSchedule',
          );
          const payments: CommunityWalletPayments = {
            address: wallet.address,
            name: wallet.name,
            paid: processPayments(resource.data['paid'], 'paid', currentEpoch),
            pending: processPayments(resource.data['scheduled'], 'pending', currentEpoch),
            vetoed: processPayments(resource.data['veto'], 'vetoed', currentEpoch),
          };

          return payments;
        } catch (error) {
          console.error(`Error processing payments for wallet ${wallet.address}:`, error);
          return {
            address: wallet.address,
            name: wallet.name,
            paid: [],
            pending: [],
            vetoed: [],
          } as CommunityWalletPayments;
        }
      }),
    );
  }

  public async getCommunityWalletsDetails(): Promise<CommunityWalletDetails[]> {
    if (this.cacheEnabled) {
      const cachedDetails = await this.getFromCache<CommunityWalletDetails[]>(
        COMMUNITY_WALLETS_DETAILS_CACHE_KEY,
      );
      if (cachedDetails) {
        return cachedDetails;
      }
    }

    const details = await this.queryCommunityWalletsDetails();
    await this.setCache(COMMUNITY_WALLETS_DETAILS_CACHE_KEY, details);

    return details;
  }

  private async queryCommunityWalletsDetails(): Promise<CommunityWalletDetails[]> {
    const wallets = await this.queryCommunityWallets();

    return await Promise.all(
      wallets.map(async (wallet) => {
        const isMultiAction = await this.queryIsMultiAction(wallet.address);
        const threshold = isMultiAction ? await this.queryThreshold(wallet.address) : undefined;
        const payees = new Set();

        let totalPaid = 0;
        const resource = await this.queryTransactions(wallet.address);
        resource.data['paid'].forEach((payment) => {
          payees.add(payment.tx.payee);
          totalPaid += Number(payment['tx']['value']);
        });
        totalPaid = formatCoin(totalPaid);

        return {
          address: wallet.address,
          name: wallet.name,
          isMultiAction,
          threshold,
          totalPaid,
          balance: wallet.balance,
          payees: payees.size,
        };
      }),
    );
  }

  private async queryIsMultiAction(address: string): Promise<boolean> {
    try {
      const res = await this.olService.aptosClient.view({
        function: '0x1::multi_action::is_multi_action',
        type_arguments: [],
        arguments: [`0x${address}`],
      });
      return res[0] as boolean;
    } catch (error) {
      console.error(`Error querying is_multi_action for address ${address}:`, error);
      return false;
    }
  }

  private async queryThreshold(address: string): Promise<number[]> {
    const res = await this.olService.aptosClient.view({
      function: '0x1::multi_action::get_threshold',
      type_arguments: [],
      arguments: [`0x${address}`],
    });
    return res as number[];
  }

  public async updateAllCaches() {
    const wallets = await this.queryCommunityWallets();
    await this.setCache(COMMUNITY_WALLETS_CACHE_KEY, wallets);

    const stats = await this.queryCommunityWalletsStats();
    await this.setCache(COMMUNITY_WALLETS_STATS_CACHE_KEY, stats);

    const payments = await this.queryCommunityWalletsPayments();
    await this.setCache(COMMUNITY_WALLETS_PAYMENTS_CACHE_KEY, payments);

    const details = await this.queryCommunityWalletsDetails();
    await this.setCache(COMMUNITY_WALLETS_DETAILS_CACHE_KEY, details);
  }
}
