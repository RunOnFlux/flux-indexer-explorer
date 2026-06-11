import type express from 'express';
import type { Server } from 'http';

export async function withTestServer(
  app: express.Application,
  run: (baseUrl: string) => Promise<void>
): Promise<void> {
  const server = await new Promise<Server>((resolve, reject) => {
    const listener = app.listen(0, '127.0.0.1');

    const onError = (error: Error) => {
      listener.off('listening', onListening);
      reject(error);
    };

    const onListening = () => {
      listener.off('error', onError);
      resolve(listener);
    };

    listener.once('error', onError);
    listener.once('listening', onListening);
  });

  const address = server.address();
  if (!address || typeof address === 'string') {
    server.close();
    throw new Error('Expected an ephemeral TCP port');
  }

  try {
    await run(`http://127.0.0.1:${address.port}`);
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  }
}

export async function readJson<T>(response: { text(): Promise<string> }): Promise<T> {
  const text = await response.text();
  return JSON.parse(text) as T;
}
