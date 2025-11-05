from dataclasses import dataclass
import time
from typing import Any
from movie.actor import AbstractBehavior, Behaviors, ActorContext, ActorSystem
from movie.actor.ref import ActorRef


class DeviceCommand: ...


class DeviceEvent: ...


class PassivateDevice(DeviceCommand): ...


@dataclass(frozen=True)
class ResponseTemperature(DeviceEvent):
    temperature: float | None
    request_id: int


@dataclass(frozen=True)
class ResponseWriteTemperature(DeviceEvent):
    request_id: int


@dataclass(frozen=True)
class ReadTemperature(DeviceCommand):
    reply_to: ActorRef[ResponseTemperature]
    request_id: int


@dataclass(frozen=True)
class RecordTemperature(DeviceCommand):
    reply_to: ActorRef[ResponseWriteTemperature]
    request_id: int
    value: float


class Device(AbstractBehavior[DeviceCommand]):
    def __init__(
        self,
        context: ActorContext[DeviceCommand],
        groupd_id: str,
        device_id: str,
    ) -> None:
        super().__init__(context)
        self._group_id = groupd_id
        self._device_id = device_id
        self._temp: float | None = None

    @staticmethod
    def create(group_id: str, device_id: str) -> AbstractBehavior[DeviceCommand]:
        return Behaviors.setup(lambda ctx: Device(ctx, group_id, device_id))

    def receive(
        self, context: ActorContext, message: DeviceCommand
    ) -> AbstractBehavior | None:
        match message:
            case ReadTemperature(reply_to, request_id):
                context.log.info(
                    f"Reading temperature {self._temp} with request id {request_id}"
                )
                reply_to.tell(ResponseTemperature(self._temp, request_id))
            case RecordTemperature(reply_to, request_id, value):
                self._temp = value
                context.log.info(
                    f"Recorded temperature {value} with request id {request_id}"
                )
                reply_to.tell(ResponseWriteTemperature(request_id))
            case PassivateDevice():
                context.log.info(f"Passivating device actor {self._device_id}")
                return Behaviors.stopped


class DeviceManagerCommand: ...


class DeviceGroupCommand: ...


@dataclass(frozen=True)
class RequestTrackDevice(DeviceManagerCommand, DeviceGroupCommand):
    group_id: str
    device_id: str
    reply_to: ActorRef


@dataclass(frozen=True)
class ResponseDeviceRegistered(DeviceManagerCommand):
    device: ActorRef[DeviceCommand]


@dataclass(frozen=True)
class DeviceTerminated(DeviceGroupCommand):
    device_id: str
    group_id: str
    device: ActorRef[DeviceCommand]


@dataclass(frozen=True)
class ReplyDeviceList(DeviceGroupCommand):
    request_id: int
    ids: set[str]


@dataclass(frozen=True)
class RequestDeviceList(DeviceGroupCommand):
    request_id: int
    reply_to: ActorRef
    group_id: str


class DeviceGroup(AbstractBehavior[DeviceGroupCommand]):
    def __init__(
        self,
        context: ActorContext[DeviceGroupCommand],
        group_id: str,
    ) -> None:
        super().__init__(context)
        self._group_id = group_id
        self._devices: dict[str, ActorRef[DeviceCommand]] = {}

    @staticmethod
    def create(group_id: str) -> AbstractBehavior[DeviceGroupCommand]:
        return Behaviors.setup(lambda ctx: DeviceGroup(ctx, group_id))

    def receive(
        self, context: ActorContext, message: DeviceGroupCommand
    ) -> AbstractBehavior | None:
        match message:
            case RequestTrackDevice(group_id, device_id, reply_to):
                if group_id == self._group_id:
                    if device_id not in self._devices:
                        device_actor = context.spawn(
                            Device.create(self._group_id, device_id), device_id
                        )
                        self._devices[device_id] = device_actor
                        context.log.info(
                            f"Created device actor for {device_id} in group {group_id}"
                        )
                    reply_to.tell(ResponseDeviceRegistered(self._devices[device_id]))
                else:
                    context.log.warning(
                        f"Ignoring TrackDevice request for {group_id}. This actor is responsible for {self._group_id}."
                    )
            case DeviceTerminated(device_id, group_id, device):
                context.log.info(
                    f"Device actor for {device_id} in group {group_id} has been terminated"
                )
                if device_id in self._devices:
                    del self._devices[device_id]
            case RequestDeviceList(request_id, reply_to, group_id):
                if group_id == self._group_id:
                    reply_to.tell(
                        ReplyDeviceList(request_id, set(self._devices.keys()))
                    )


class IotSupervisor(AbstractBehavior[None]):
    def __init__(self, context: ActorContext[None]) -> None:
        super().__init__(context)
        context.log.info("IoT Supervisor started")
        self._device_group = context.spawn(
            DeviceGroup.create("group-1"), "device-group-1"
        )
        self._device_group.tell(
            RequestTrackDevice("group-1", "device-1", context.get_self())
        )

    def receive(self, context: ActorContext, message: Any) -> AbstractBehavior | None:
        match message:
            case ResponseDeviceRegistered(device):
                context.log.info(f"Device registered: {device}")
                device.tell(RecordTemperature(context.get_self(), 1, 23.5))

    @staticmethod
    def create() -> AbstractBehavior[None]:
        return Behaviors.setup(lambda ctx: IotSupervisor(ctx))


def main():
    system = ActorSystem.create(IotSupervisor.create(), "iot-supervisor")
    time.sleep(2)
    system.stop()


if __name__ == "__main__":
    main()
