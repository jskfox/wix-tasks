import { config } from '../config';

export abstract class BaseTask {
  abstract readonly name: string;
  abstract readonly description: string;
  abstract readonly cronExpression: string;
  readonly timezone: string = config.timezone;

  abstract execute(): Promise<void>;
}
