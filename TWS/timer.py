import threading

class Timer(threading.Thread):
    
    def __init__(self, event):
        threading.Thread.__init__(self)
        self.stopped = event

    def run(self):
        while not self.stopped.wait(1):
            #sheet.update([market_data_df.columns.values.tolist()] + market_data_df.values.tolist())
            print("Updating market data.")