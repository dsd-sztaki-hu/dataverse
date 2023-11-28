class CrossWindowMessenger {
  constructor(timeoutCheckInterval = 60000) {
    this.windowId = Date.now() + Math.random().toString(36).substr(2, 9);
    this.messageSubscriptions = {};
    this.listenToStorageEvents();
    setInterval(() => this.checkMessageTimeouts(), timeoutCheckInterval);
  }

  listenToStorageEvents() {
    window.addEventListener('storage', (event) => {
      if (event.key.startsWith('message_')) {
        const message = JSON.parse(localStorage.getItem(event.key));
        if (message && this.messageSubscriptions[message.type]) {
          this.messageSubscriptions[message.type].forEach(subscription => {
            if (subscription.datasetId === undefined || subscription.datasetId === message.datasetId) {
              subscription.callback(message.content);
            }
          });
        }
      }
    });
  }

  subscribeToMessageType(messageType, datasetId, callback) {
    const subscription = { callback, datasetId };
    if (!this.messageSubscriptions[messageType]) {
      this.messageSubscriptions[messageType] = [];
    }
    this.messageSubscriptions[messageType].push(subscription);
  }

  unsubscribeFromMessageType(messageType, callback) {
    if (this.messageSubscriptions[messageType]) {
      this.messageSubscriptions[messageType] = this.messageSubscriptions[messageType].filter(subscription => subscription.callback !== callback);
    }
  }

  publishMessage(messageType, datasetId, content, timeout = 30000) {
    const messageKey = `message_${messageType}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    const message = {
      type: messageType,
      content: content,
      timestamp: Date.now(),
      timeout: timeout,
      datasetId: datasetId,
      windowId: this.windowId,
    };
    localStorage.setItem(messageKey, JSON.stringify(message));
  }

  checkMessageTimeouts() {
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key.startsWith('message_')) {
        const message = JSON.parse(localStorage.getItem(key));
        if (Date.now() - message.timestamp > message.timeout) {
          localStorage.removeItem(key);
        }
      }
    }
  }
}
