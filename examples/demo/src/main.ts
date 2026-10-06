import 'reflect-metadata';
import { join } from 'node:path';
import { NestFactory } from '@nestjs/core';
import { NestExpressApplication } from '@nestjs/platform-express';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    logger: ['warn', 'error'],
  });
  // Lets the consumers finish their in-flight messages on Ctrl+C.
  app.enableShutdownHooks();
  app.useStaticAssets(join(__dirname, '..', 'public'));

  const port = Number(process.env.PORT ?? 4100);
  await app.listen(port);
  console.log(`Playground: http://localhost:${port}`);
}

void bootstrap();
