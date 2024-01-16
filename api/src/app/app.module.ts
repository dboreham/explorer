import { join } from "node:path";
import { Module } from "@nestjs/common";
import { GraphQLModule as NestGraphQLModule } from "@nestjs/graphql";
import { ConfigModule } from "@nestjs/config";
import { ApolloDriver, ApolloDriverConfig } from "@nestjs/apollo";

import config from "../config/config.js";
import { AppService } from "./app.service.js";
import { ClickhouseModule } from "../clickhouse/clickhouse.module.js";
import { OlModule } from "../ol/ol.module.js";
import { S3Module } from "../s3/s3.module.js";
import { NodeWatcherModule } from "../node-watcher/node-watcher.module.js";
import { GraphQLModule } from "../graphql/graphql.module.js";

@Module({
  imports: [
    GraphQLModule,

    ConfigModule.forRoot({
      isGlobal: true,
      load: [config],
    }),

    NestGraphQLModule.forRoot<ApolloDriverConfig>({
      driver: ApolloDriver,
      autoSchemaFile: join(process.cwd(), "src/schema.gql"),
      playground: true,
    }),

    S3Module,
    ClickhouseModule,
    OlModule,
    NodeWatcherModule,
  ],
  controllers: [],
  providers: [AppService],
})
export class AppModule {}
