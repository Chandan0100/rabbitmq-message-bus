import { randomUUID } from 'node:crypto';
import { Injectable } from '@nestjs/common';
import { DestinationRegistry } from 'rabbitmq-message-bus';
import { DemoDatabase } from '../infrastructure/demo-database';
import {
  TASK_ASSIGNED,
  TASK_COMPLETED,
  TASK_CREATED,
} from '../infrastructure/messaging.config';

interface Task {
  id: string;
  title: string;
  assignee: string | null;
  status: string;
}

const TITLES = ['Review pull request', 'Write release notes', 'Fix login bug', 'Plan the sprint', 'Update dependencies', 'Call the customer'];
const PEOPLE = ['asha@example.com', 'bruno@example.com', 'chen@example.com', 'dana@example.com'];

const pick = <T>(items: T[]): T => items[Math.floor(Math.random() * items.length)];

/**
 * The business side. It never talks to RabbitMQ: it changes a task and records
 * the event in the outbox, both in one transaction. The dispatcher publishes
 * the outbox later.
 */
@Injectable()
export class TasksService {
  constructor(
    private readonly db: DemoDatabase,
    private readonly destinations: DestinationRegistry,
  ) {}

  createTask(title = pick(TITLES)): Task {
    const task: Task = { id: randomUUID(), title, assignee: null, status: 'open' };

    this.db.transaction(() => {
      this.db.run(
        'insert into task (id, title, assignee, status, created_at) values (?, ?, ?, ?, ?)',
        task.id,
        task.title,
        task.assignee,
        task.status,
        new Date().toISOString(),
      );
      this.recordEvent(TASK_CREATED, { task_uuid: task.id, title: task.title });
    });

    return task;
  }

  assignTask(task: Task, assignee = pick(PEOPLE)): Task {
    this.db.transaction(() => {
      this.db.run('update task set assignee = ? where id = ?', assignee, task.id);
      this.recordEvent(TASK_ASSIGNED, {
        task_uuid: task.id,
        title: task.title,
        assignee,
      });
    });

    return { ...task, assignee };
  }

  completeTask(task: Task): Task {
    this.db.transaction(() => {
      this.db.run("update task set status = 'done' where id = ?", task.id);
      this.recordEvent(TASK_COMPLETED, {
        task_uuid: task.id,
        title: task.title,
        assignee: task.assignee,
      });
    });

    return { ...task, status: 'done' };
  }

  findOpenTask(assigned: boolean): Task | undefined {
    return this.db.get<Task>(
      `select * from task where status = 'open' and assignee is ${
        assigned ? 'not null' : 'null'
      } order by random() limit 1`,
    );
  }

  /** Adds the event to the outbox. Must run inside the transaction of the change it describes. */
  private recordEvent(type: string, body: Record<string, unknown>): void {
    const messageId = randomUUID();
    const { exchange, routingKey } = this.destinations.resolve(type);

    this.db.run(
      'insert into outbox_message (message_id, type, exchange, routing_key, properties, body, created_at) values (?, ?, ?, ?, ?, ?, ?)',
      messageId,
      type,
      exchange,
      routingKey,
      JSON.stringify({
        messageId,
        type,
        appId: 'demo.tasks',
        contentType: 'application/json',
        headers: { type },
      }),
      JSON.stringify({ uuid: messageId, fired_at: new Date().toISOString(), ...body }),
      new Date().toISOString(),
    );
  }
}
