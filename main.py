from movie.actor import AbstractBehavior, Behaviors, ActorContext, ActorSystem


class IotSupervisor(AbstractBehavior[None]):
    def __init__(self, context: ActorContext[None]) -> None:
        super().__init__(context)

    @staticmethod
    def create() -> AbstractBehavior[None]:
        return Behaviors.setup(lambda ctx: IotSupervisor(ctx))


def main():
    system = ActorSystem.create(IotSupervisor.create(), "iot-supervisor")
    system.tell(None)
    system.stop()


if __name__ == "__main__":
    main()
