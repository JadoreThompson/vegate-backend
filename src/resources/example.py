client = ...
loader = Cls(client, ...)
broker = Cls(client)
strat = Strat()
runner = Cls(strat, brokr)

for ctx in obj:
    runner.run(ctx)