export class InferenceCapacityError extends Error {
  public constructor(message = 'Inference queue is full') {
    super(message);
    this.name = 'InferenceCapacityError';
  }
}

export class InferenceQueueTimeoutError extends Error {
  public constructor(message = 'Inference request timed out while queued') {
    super(message);
    this.name = 'InferenceQueueTimeoutError';
  }
}

export class InferenceExecutionTimeoutError extends Error {
  public constructor(message = 'Inference request timed out during execution') {
    super(message);
    this.name = 'InferenceExecutionTimeoutError';
  }
}

export class ModelWorkerFailureError extends Error {
  public constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = 'ModelWorkerFailureError';
  }
}
