def test_evolve():

    """
    config
    IEvolve(config) -> the whole evolution
    IEvolve(sequence) -> iterate over list and return a new list with resolved items
    IEvolve(mapping) -> iterate over mapping

    we should introduce a kind of request,
    that resembles the CLI config or a kind of an incomming request from somewhere else to process

    A Request:
    - is unique by spec definition, target connection identifier
    - should wait until a similar unique request has finished



    so there must be somthing like IRequest, IResponse

    ICliConfig IRequest(ICliConfig)

    utility: IRequestDispatcher

    """
