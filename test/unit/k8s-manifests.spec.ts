import { parse } from 'yaml';
import { generateWorkerManifests } from '../../src/k8s';

const placeholders = { KUB_SERVICE_NAME: 'padmasana-service' };

describe('generateWorkerManifests', () => {
  it('builds a CronJob that dispatches the outbox of a module every minute', () => {
    const [cronJob] = generateWorkerManifests({
      placeholders,
      dispatchers: [{ module: 'identity-and-access-management', alias: 'iam' }],
    });

    expect(cronJob.fileName).toBe(
      'dispatch-messages.identity-and-access-management.cronjob.yml',
    );
    expect(parse(cronJob.yaml)).toEqual({
      apiVersion: 'batch/v1',
      kind: 'CronJob',
      metadata: {
        name: '{KUB_SERVICE_NAME}-iam-dispatch-cronjob',
        namespace: '{KUB_NAMESPACE_PREFIX}-{BUILD_ENV}',
        labels: { app: '{KUB_SERVICE_NAME}-identity-and-access-management' },
      },
      spec: {
        schedule: '*/1 * * * *',
        concurrencyPolicy: 'Forbid',
        jobTemplate: {
          spec: {
            template: {
              spec: {
                containers: [
                  {
                    name: '{KUB_SERVICE_NAME}-iam-dispatcher',
                    image: 'localhost:32000/image:latest',
                    imagePullPolicy: 'Always',
                    envFrom: [
                      { configMapRef: { name: '{KUB_SERVICE_NAME}-config' } },
                      { secretRef: { name: '{KUB_SERVICE_NAME}-secrets' } },
                      { secretRef: { name: 'rabbitmq-credentials' } },
                    ],
                    command: [
                      '/bin/sh',
                      '-c',
                      'npm run dispatch-messages -- --limit 10 --module=identity-and-access-management --schema=identity-and-access-management',
                    ],
                    resources: {
                      requests: { cpu: '100m', memory: '256Mi' },
                      limits: { cpu: '250m', memory: '512Mi' },
                    },
                  },
                ],
                restartPolicy: 'OnFailure',
                imagePullSecrets: [{ name: 'gcr-secret' }],
              },
            },
          },
        },
      },
    });
  });

  it('builds a Deployment that consumes the queue of a module', () => {
    const volumes = [
      { name: 'mailer', secret: { secretName: 'mailer-service-account' } },
    ];
    const volumeMounts = [
      { name: 'mailer', mountPath: '/app/etc/secrets/mailer.json', readOnly: true },
    ];

    const [deployment] = generateWorkerManifests({
      consumers: [{ module: 'notification-module', volumes, volumeMounts, replicas: 2 }],
    });
    const manifest = parse(deployment.yaml);

    expect(deployment.fileName).toBe('handle-messages.notification-module.yml');
    expect(manifest.metadata).toEqual({
      labels: { app: '{KUB_SERVICE_NAME}-notification-module-handler' },
      name: '{KUB_SERVICE_NAME}-notification-module-handler-deployment',
      namespace: '{KUB_NAMESPACE_PREFIX}-{BUILD_ENV}',
    });
    expect(manifest.spec.replicas).toBe(2);
    expect(manifest.spec.selector.matchLabels).toEqual({
      app: '{KUB_SERVICE_NAME}-notification-module-handler',
    });
    expect(manifest.spec.template.metadata.labels.app).toBe(
      '{KUB_SERVICE_NAME}-notification-module-handler',
    );
    expect(manifest.spec.template.spec.volumes).toEqual(volumes);
    expect(manifest.spec.template.spec.containers[0]).toMatchObject({
      name: '{KUB_SERVICE_NAME}-notification-module-handler-container',
      volumeMounts,
      command: [
        '/bin/sh',
        '-c',
        'npm run handle-messages -- --limit 10 --module=notification-module --schema=notification-module',
      ],
    });
  });

  it('runs the worker without a shell when an entrypoint is given', () => {
    const [deployment] = generateWorkerManifests({
      entrypoint: ['dumb-init', 'node', 'dist/message-bus-cli.js'],
      consumers: [
        {
          module: 'audit',
          schema: false,
          limit: 25,
          args: ['--primary-queue', 'padmasana.audit'],
          terminationGracePeriodSeconds: 45,
        },
      ],
    });
    const { spec } = parse(deployment.yaml).spec.template;

    expect(spec.terminationGracePeriodSeconds).toBe(45);
    expect(spec.containers[0].command).toEqual([
      'dumb-init',
      'node',
      'dist/message-bus-cli.js',
      'handle-messages',
      '--limit',
      '25',
      '--module=audit',
      '--primary-queue',
      'padmasana.audit',
    ]);
  });

  it('keeps the placeholders quoted so the YAML stays valid before substitution', () => {
    const [cronJob] = generateWorkerManifests({
      dispatchers: [{ module: 'board-management' }],
    });

    expect(cronJob.yaml).toContain(
      'name: "{KUB_SERVICE_NAME}-board-management-dispatch-cronjob"',
    );
  });

  it('refuses a CronJob name longer than Kubernetes allows', () => {
    expect(() =>
      generateWorkerManifests({
        placeholders,
        dispatchers: [{ module: 'identity-and-access-management' }],
      }),
    ).toThrow(
      'CronJob name "padmasana-service-identity-and-access-management-dispatch-cronjob" is 65 characters long, the limit is 52. Give the worker a shorter alias.',
    );
  });

  it('refuses the same worker declared twice', () => {
    expect(() =>
      generateWorkerManifests({
        consumers: [{ module: 'tasks' }, { module: 'tasks' }],
      }),
    ).toThrow('Declared twice: consumer tasks.');
  });
});
