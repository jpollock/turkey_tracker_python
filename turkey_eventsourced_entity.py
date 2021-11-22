"""
Copyright 2020 Lightbend Inc.
Licensed under the Apache License, Version 2.0.
"""

from dataclasses import dataclass, field
from typing import MutableSet

from google.protobuf.empty_pb2 import Empty

from akkaserverless.event_sourced_context import EventSourcedCommandContext
from akkaserverless.event_sourced_entity import EventSourcedEntity
from turkey_domain_pb2 import (TurkeyState, InOven, OutOfOven, TemperatureChange, DESCRIPTOR as DOMAIN_DESCRIPTOR)
from turkey_api_pb2 import (CookingCommand, TemperatureChangeCommand,  _TURKEYSERVICE, DESCRIPTOR as API_DESCRIPTOR)

def init(entity_id: str) -> TurkeyState:
    cs = TurkeyState()
    return cs


entity = EventSourcedEntity(_TURKEYSERVICE, [API_DESCRIPTOR, DOMAIN_DESCRIPTOR], 'turkeys', init)

'''
# Event Sourced
'''
@entity.command_handler("StartCooking")
def start_cooking(state: TurkeyState, command: CookingCommand, context: EventSourcedCommandContext):
    np = InOven(turkey_id= command.turkey_id)
    context.emit(np)
    return Empty()

@entity.command_handler("EndCooking")
def end_cooking(state: TurkeyState, command: CookingCommand, context: EventSourcedCommandContext):
    np = OutOfOven(turkey_id= command.turkey_id)
    context.emit(np)
    return Empty()

@entity.command_handler("IncreaseOvenTemperature")
def increase_oven_temperature(state: TurkeyState, command: TemperatureChangeCommand, context: EventSourcedCommandContext):
    np = TemperatureChange(turkey_id= command.turkey_id, type=TemperatureChange.Type.EXTERNAL, new_temperature=(state.external_temperature + command.temperature_change))
    context.emit(np)
    return Empty()

@entity.command_handler("DecreaseOvenTemperature")
def decrease_oven_temperature(state: TurkeyState, command: TemperatureChangeCommand, context: EventSourcedCommandContext):
    np = TemperatureChange(turkey_id= command.turkey_id, type=TemperatureChange.Type.EXTERNAL, new_temperature=(state.external_temperature - command.temperature_change))
    context.emit(np)
    return Empty()

@entity.command_handler("GetCurrentTurkey")
def get(state: TurkeyState):
    return state

@entity.event_handler(InOven)
def in_oven(state: TurkeyState, event: InOven ):
    state.in_oven = True
    return state

@entity.event_handler(OutOfOven)
def out_of_oven(state: TurkeyState, event: OutOfOven ):
    state.in_oven = False
    return state

@entity.event_handler(TemperatureChange)
def temperature_changed(state: TurkeyState, event: TemperatureChange ):
    if event.type == TemperatureChange.Type.EXTERNAL:
        state.external_temperature = event.new_temperature
    elif event.type == TemperatureChange.Type.INTERNAL:
        state.internal_temperature = event.new_temperature
    return state
