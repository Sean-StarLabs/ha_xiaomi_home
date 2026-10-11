# -*- coding: utf-8 -*-
"""Washer operations for Xiaomi Home."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import logging
import math
from typing import Any, Optional

import voluptuous as vol

from homeassistant.auth.permissions.const import POLICY_CONTROL
from homeassistant.const import ATTR_DEVICE_ID, STATE_OFF, STATE_ON
from homeassistant.core import HomeAssistant, ServiceCall, State
from homeassistant.exceptions import (
    HomeAssistantError,
    ServiceValidationError,
    Unauthorized,
    UnknownUser,
)
from homeassistant.helpers import (
    config_validation as cv,
    device_registry,
    entity_registry,
)
from homeassistant.util import dt as dt_util

from .miot.const import DOMAIN
from .miot.miot_device import MIoTDevice
from .miot.miot_spec import MIoTSpecAction, MIoTSpecProperty, MIoTSpecService

_LOGGER = logging.getLogger(__name__)

SERVICE_START_WASHER = 'start_washer'
ATTR_DELAY_UNTIL_ENTITY_ID = 'delay_until_entity_id'

REPORT_TIMEOUT = 30
START_TIMEOUT = 30

WASHER_SETTING_NAMES = (
    'mode',
    'target-temperature',
    'rinsh-times',
    'spin-speed',
    'soak-time',
    'wash-time',
    'steam-sterilization',
    'high-water-switch',
    'sleep-mode',
    'drying-level',
    'drying-time',
    'drying-degree',
    'reservation-wash-status',
    'reservation-left-time',
    'one-click-wash',
    'ai-mode',
    'laundry-beads',
)

MANAGEMENT_SETTING_NAMES = {
    'detergent-management': (
        'detergent-self-delivery',
        'detergent-self-delivery-level',
    ),
    'fabric-softener-management': (
        'fabric-softener-self-delivery',
        'softener-self-delivery-level',
        'disinfectant-mode',
    ),
}

START_WASHER_SCHEMA = vol.Schema({
    vol.Required(ATTR_DEVICE_ID): vol.All(cv.ensure_list, [cv.string]),
    vol.Optional(ATTR_DELAY_UNTIL_ENTITY_ID): cv.entity_id,
})

_DEVICE_LOCKS: dict[str, asyncio.Lock] = {}
_ACTIVE_TRANSACTIONS: dict[str, set[asyncio.Task]] = {}
_UNLOADING_ENTRIES: set[str] = set()


def _find_service(device: MIoTDevice, name: str) -> Optional[MIoTSpecService]:
    return next(
        (service for service in device.spec_instance.services
         if service.name == name),
        None)


def _value_by_name(prop: MIoTSpecProperty, name: str) -> Any:
    if not prop.value_list:
        return None
    return next(
        (item.value for item in prop.value_list.items if item.name == name),
        None)


def _state_value(prop: MIoTSpecProperty, state: State) -> Any:
    if state.state in {'unknown', 'unavailable'}:
        raise ServiceValidationError(
            f'{state.name} is {state.state}; the washer was not started')
    if prop.format_ is bool:
        if state.state == STATE_ON:
            return True
        if state.state == STATE_OFF:
            return False
        raise ServiceValidationError(
            f'{state.name} has an invalid switch state: {state.state}')
    if prop.value_list:
        value = prop.value_list.get_value_by_description(state.state)
        if value is None:
            raise ServiceValidationError(
                f'{state.name} has an unsupported value: {state.state}')
        return value
    try:
        return prop.value_precision(prop.value_format(state.state))
    except (TypeError, ValueError) as err:
        raise ServiceValidationError(
            f'{state.name} has an invalid value: {state.state}') from err

class WasherTransaction:
    """Apply and verify a washer configuration before starting it."""

    def __init__(self, hass: HomeAssistant, device: MIoTDevice) -> None:
        self._hass = hass
        self._device = device
        self._entity_registry = entity_registry.async_get(hass)
        self._washer = _find_service(device, 'washer')
        if self._washer is None:
            raise ServiceValidationError(
                f'{device.name} does not expose a washer service')

        self._props = {prop.name: prop for prop in self._washer.properties}
        self._actions = {
            action.name: action for action in self._washer.actions}
        required_props = {'status', 'door-state'}
        required_actions = {'set-customize-params', 'start-wash'}
        missing = (
            required_props.difference(self._props)
            | required_actions.difference(self._actions)
        )
        if missing:
            raise ServiceValidationError(
                f'{device.name} is missing washer capabilities: '
                f'{", ".join(sorted(missing))}')
        self._report_event = asyncio.Event()
        self._report_subscriptions: list[
            tuple[MIoTSpecProperty, int]] = []

    def _setting_properties(self) -> list[MIoTSpecProperty]:
        settings = [
            self._props[name] for name in WASHER_SETTING_NAMES
            if name in self._props
            and self._props[name].readable
            and self._props[name].writable
            and self._props[name].platform
        ]
        for service_name, prop_names in MANAGEMENT_SETTING_NAMES.items():
            service = _find_service(self._device, service_name)
            if service is None:
                continue
            props = {prop.name: prop for prop in service.properties}
            settings.extend(
                props[name] for name in prop_names
                if name in props
                and props[name].readable
                and props[name].writable
                and props[name].platform)
        return [
            prop for prop in settings
            if self._entity_state(prop) is not None]

    def _entity_state(self, prop: MIoTSpecProperty) -> Optional[State]:
        unique_id = self._device.gen_prop_entity_id(
            ha_domain=DOMAIN,
            spec_name=prop.name,
            siid=prop.service.iid,
            piid=prop.iid)
        entity_id = self._entity_registry.async_get_entity_id(
            prop.platform, DOMAIN, unique_id)
        if entity_id is None:
            return None
        return self._hass.states.get(entity_id)

    def _current_state(self, prop: MIoTSpecProperty) -> State:
        state = self._entity_state(prop)
        if state is None:
            raise ServiceValidationError(
                f'The washer setting {prop.description_trans} is not loaded')
        return state

    def _capture_desired(
        self, settings: list[MIoTSpecProperty]
    ) -> dict[MIoTSpecProperty, Any]:
        desired = {}
        by_name = {prop.name: prop for prop in settings}
        controller_names = {
            'drying-level',
            'reservation-wash-status',
            'detergent-self-delivery',
            'fabric-softener-self-delivery',
        }
        for name in controller_names:
            prop = by_name.get(name)
            if prop is not None:
                desired[prop] = _state_value(
                    prop, self._current_state(prop))

        drying_level = self._props.get('drying-level')
        excluded = set()
        if drying_level is not None and drying_level in desired:
            level = desired[drying_level]
            if level == _value_by_name(drying_level, 'none'):
                excluded.update({'drying-time', 'drying-degree'})
            elif level != _value_by_name(drying_level, 'timed'):
                excluded.add('drying-time')
        else:
            excluded.update({'drying-time', 'drying-degree'})

        reservation = self._props.get('reservation-wash-status')
        if (
            reservation in desired
            and desired[reservation] == _value_by_name(reservation, 'none')
        ):
            excluded.add('reservation-left-time')
        elif reservation not in desired:
            excluded.add('reservation-left-time')

        detergent = by_name.get('detergent-self-delivery')
        if detergent is None or desired[detergent] is False:
            excluded.add('detergent-self-delivery-level')

        softener = by_name.get('fabric-softener-self-delivery')
        if softener is None or desired[softener] is False:
            excluded.add('softener-self-delivery-level')

        for prop in settings:
            if prop.name in excluded or prop in desired:
                continue
            desired[prop] = _state_value(prop, self._current_state(prop))
        return desired

    def _delay_target(
        self, desired: dict[MIoTSpecProperty, Any], entity_id: Optional[str]
    ) -> Optional[datetime]:
        if not entity_id:
            return None
        reservation = self._props.get('reservation-wash-status')
        delay = self._props.get('reservation-left-time')
        if reservation not in desired:
            raise ServiceValidationError(
                'The washer does not expose the selected reservation settings')
        if desired[reservation] == _value_by_name(reservation, 'none'):
            raise ServiceValidationError(
                'Select Start Wash or Finish Wash before setting a delay')
        if delay not in desired:
            raise ServiceValidationError(
                'The washer does not expose the selected reservation settings')
        state = self._hass.states.get(entity_id)
        if state is None or state.state in {'unknown', 'unavailable'}:
            raise ServiceValidationError(
                'The washer delay time is unavailable')
        target = dt_util.parse_datetime(state.state)
        if target is None:
            raise ServiceValidationError(
                'The washer delay time is invalid')
        if target.tzinfo is None:
            target = target.replace(tzinfo=dt_util.DEFAULT_TIME_ZONE)
        return target.astimezone(UTC)

    def _apply_delay_until(
        self,
        desired: dict[MIoTSpecProperty, Any],
        target: Optional[datetime],
    ) -> None:
        if target is None:
            return
        delay = self._props['reservation-left-time']
        minutes = math.ceil(
            (target - datetime.now(UTC)).total_seconds() / 60)
        if minutes < 1 or minutes > 2880:
            raise ServiceValidationError(
                'The washer delay time must be within the next 48 hours')
        desired[delay] = delay.value_precision(delay.value_format(minutes))

    def _on_report(self, params: dict, _: Any) -> None:
        if not params.get('_from_cloud_cache'):
            self._report_event.set()

    def _subscribe_reports(self, props: list[MIoTSpecProperty]) -> None:
        for prop in dict.fromkeys(props):
            sub_id = self._device.sub_property(
                handler=self._on_report,
                siid=prop.service.iid,
                piid=prop.iid)
            self._report_subscriptions.append((prop, sub_id))

    def _unsubscribe_reports(self) -> None:
        for prop, sub_id in self._report_subscriptions:
            self._device.unsub_property(
                siid=prop.service.iid, piid=prop.iid, sub_id=sub_id)
        self._report_subscriptions.clear()

    async def _read_local_values(
        self,
        props: list[MIoTSpecProperty],
        values: dict[MIoTSpecProperty, Any],
    ) -> None:
        for prop in props:
            result = await self._device.miot_client.refresh_prop_local_async(
                did=self._device.did,
                siid=prop.service.iid,
                piid=prop.iid)
            if result is None:
                continue
            value = prop.value_format(result['value'])
            value = prop.eval_expr(value)
            values[prop] = prop.value_precision(value)

    async def _read_before(
        self, props: list[MIoTSpecProperty], deadline: float
    ) -> dict[MIoTSpecProperty, Any]:
        values = {}
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return values
        try:
            await asyncio.wait_for(
                self._read_local_values(props, values), timeout=remaining)
        except TimeoutError:
            pass
        return values

    async def _preflight_values(
        self, props: list[MIoTSpecProperty]
    ) -> dict[MIoTSpecProperty, Any]:
        deadline = asyncio.get_running_loop().time() + REPORT_TIMEOUT
        observations = await self._read_before(props, deadline)
        missing = [
            prop.description_trans for prop in props
            if prop not in observations
        ]
        if missing:
            raise HomeAssistantError(
                'Reliable local readback is unavailable for these washer '
                f'properties: {", ".join(missing)}; the washer was not changed')
        return observations

    async def _wait_for_values(
        self,
        expected: dict[MIoTSpecProperty, Any],
        timeout: int = REPORT_TIMEOUT,
    ) -> dict[MIoTSpecProperty, Any]:
        deadline = asyncio.get_running_loop().time() + timeout
        actual = {}
        while asyncio.get_running_loop().time() < deadline:
            props = list(expected)
            actual = await self._read_before(props, deadline)
            if all(
                actual.get(prop) == value
                for prop, value in expected.items()
            ):
                return actual
            self._report_event.clear()
            try:
                await asyncio.wait_for(
                    self._report_event.wait(),
                    timeout=min(
                        2, max(0, deadline - asyncio.get_running_loop().time())))
            except TimeoutError:
                pass
        mismatches = [
            prop.description_trans
            for prop, value in expected.items()
            if actual.get(prop) != value
        ]
        raise HomeAssistantError(
            'The washer rejected these settings: '
            f'{", ".join(mismatches)}')

    async def _write_property(
        self, prop: MIoTSpecProperty, value: Any
    ) -> None:
        await self._device.miot_client.set_prop_async(
            did=self._device.did,
            siid=prop.service.iid,
            piid=prop.iid,
            value=value)

    async def _run_action(self, action: MIoTSpecAction) -> None:
        await self._device.action_async(
            siid=action.service.iid, aiid=action.iid, in_list=[])

    async def _wait_for_status(
        self, expected: set[Any]
    ) -> Any:
        status = self._props['status']
        deadline = asyncio.get_running_loop().time() + START_TIMEOUT
        while asyncio.get_running_loop().time() < deadline:
            result = await self._read_before([status], deadline)
            value = result.get(status)
            if value is not None and value in expected:
                return value
            self._report_event.clear()
            try:
                await asyncio.wait_for(
                    self._report_event.wait(),
                    timeout=min(
                        2, max(0, deadline - asyncio.get_running_loop().time())))
            except TimeoutError:
                pass
        raise HomeAssistantError(
            'The start command was sent, but the washer did not report '
            'that it started')

    async def async_start(self, delay_until_entity_id: Optional[str]) -> None:
        if not self._device.online:
            raise HomeAssistantError(f'{self._device.name} is offline')

        settings = self._setting_properties()
        desired = self._capture_desired(settings)
        delay_target = self._delay_target(desired, delay_until_entity_id)
        self._apply_delay_until(desired, delay_target)

        status = self._props['status']
        door = self._props['door-state']
        on = self._props.get('on')
        child_lock = self._props.get('child-protected-enabled')
        preflight_props = [status, door, *desired]
        if child_lock and child_lock.readable:
            preflight_props.append(child_lock)
        preflight_props = list(dict.fromkeys(preflight_props))

        self._subscribe_reports([*preflight_props, *desired])
        try:
            await self._start_transaction(
                desired=desired,
                preflight=await self._preflight_values(preflight_props),
                status=status,
                door=door,
                on=on,
                child_lock=child_lock,
                delay_target=delay_target)
        finally:
            self._unsubscribe_reports()

    async def _start_transaction(
        self,
        desired: dict[MIoTSpecProperty, Any],
        preflight: dict[MIoTSpecProperty, Any],
        status: MIoTSpecProperty,
        door: MIoTSpecProperty,
        on: Optional[MIoTSpecProperty],
        child_lock: Optional[MIoTSpecProperty],
        delay_target: Optional[datetime],
    ) -> None:
        status_off = _value_by_name(status, 'off')
        status_idle = _value_by_name(status, 'idle')
        status_completed = _value_by_name(status, 'completed')
        status_busy = _value_by_name(status, 'busy')
        status_delay = _value_by_name(status, 'delay')
        idle_states = {
            value for value in (status_idle, status_completed)
            if value is not None
        }
        running_states = {
            value for value in (status_busy, status_delay)
            if value is not None
        }
        closed_states = {
            value for value in (
                _value_by_name(door, 'closed_and_can_be_opened'),
                _value_by_name(door, 'closed_and_can_not_be_opened'),
            )
            if value is not None
        }
        if preflight[door] not in closed_states:
            raise ServiceValidationError(
                'The washer door is not closed; the washer was not started')

        child_lock_was_enabled = False
        primary_error: Optional[BaseException] = None
        try:
            if preflight[status] == status_off:
                if on is None or not on.writable:
                    raise HomeAssistantError(
                        'The washer is off and cannot be woken')
                await self._write_property(on, True)
                preflight[status] = await self._wait_for_status(
                    idle_states)
            if preflight[status] not in idle_states:
                raise ServiceValidationError(
                    'The washer is not idle; its settings were not changed')

            child_lock_was_enabled = bool(
                child_lock is not None and preflight.get(child_lock) is True)
            if child_lock_was_enabled and child_lock is not None:
                await self._write_property(child_lock, False)
                await self._wait_for_values({child_lock: False})

            mode = self._props.get('mode')
            if mode in desired:
                await self._write_property(mode, desired[mode])
                await self._wait_for_values({mode: desired[mode]})

            self._apply_delay_until(desired, delay_target)
            for prop, value in desired.items():
                if prop is mode:
                    continue
                await self._write_property(prop, value)

            await self._run_action(self._actions['set-customize-params'])
            await self._wait_for_values(desired)

            await self._run_action(self._actions['start-wash'])
            await self._wait_for_status(running_states)
        except BaseException as err:
            primary_error = err
            raise
        finally:
            if child_lock_was_enabled and child_lock is not None:
                restore_task = asyncio.create_task(
                    self._restore_child_lock(child_lock))
                self._device.miot_client.authorize_device_operation_task(
                    self._device.did, restore_task)
                cancelled = False
                restore_error = None
                while not restore_task.done():
                    try:
                        await asyncio.shield(restore_task)
                    except asyncio.CancelledError:
                        cancelled = True
                    except Exception as err:
                        restore_error = err
                        break
                if restore_error is None and restore_task.done():
                    try:
                        restore_task.result()
                    except Exception as err:
                        restore_error = err
                if restore_error is not None:
                    if primary_error is None:
                        raise restore_error
                    _LOGGER.exception(
                        'Failed to restore the child lock after a washer '
                        'transaction error', exc_info=restore_error)
                if cancelled:
                    raise asyncio.CancelledError

    async def _restore_child_lock(self, child_lock: MIoTSpecProperty) -> None:
        await self._write_property(child_lock, True)
        await self._wait_for_values({child_lock: True})


def _resolve_device(
    hass: HomeAssistant, device_id: str
) -> tuple[str, MIoTDevice]:
    registry_entry = device_registry.async_get(hass).async_get(device_id)
    if registry_entry is None:
        raise ServiceValidationError(f'Device not found: {device_id}')
    identifiers = {
        identifier for domain, identifier in registry_entry.identifiers
        if domain == DOMAIN
    }
    for entry_id, devices in hass.data[DOMAIN]['devices'].items():
        for device in devices:
            if device.did_tag in identifiers:
                return entry_id, device
    raise ServiceValidationError(
        'The selected device is not loaded by Xiaomi Home')


async def async_register_washer_service(hass: HomeAssistant) -> None:
    """Register the washer transaction service."""
    if hass.services.has_service(DOMAIN, SERVICE_START_WASHER):
        return

    async def async_start_washer(call: ServiceCall) -> None:
        device_ids: list[str] = call.data[ATTR_DEVICE_ID]
        if len(device_ids) != 1:
            raise ServiceValidationError(
                'Select exactly one Xiaomi washer')
        device_id = device_ids[0]
        if call.context.user_id:
            user = await hass.auth.async_get_user(call.context.user_id)
            if user is None:
                raise UnknownUser(context=call.context)
            if not user.is_admin:
                registry = entity_registry.async_get(hass)
                entries = [
                    entry for entry in entity_registry.async_entries_for_device(
                        registry,
                        device_id,
                        include_disabled_entities=True)
                    if entry.platform == DOMAIN
                ]
                if not entries:
                    raise Unauthorized(
                        context=call.context,
                        permission=POLICY_CONTROL)
                for entry in entries:
                    if (
                        not user.permissions.check_entity(
                            entry.entity_id, POLICY_CONTROL)
                    ):
                        raise Unauthorized(
                            context=call.context,
                            entity_id=entry.entity_id,
                            permission=POLICY_CONTROL)
        entry_id, device = _resolve_device(hass, device_id)
        if entry_id in _UNLOADING_ENTRIES:
            raise ServiceValidationError(
                'The Xiaomi Home entry is being reloaded')
        lock = _DEVICE_LOCKS.setdefault(device.did, asyncio.Lock())
        if lock.locked():
            raise ServiceValidationError(
                'A washer settings transaction is already running')
        task = asyncio.current_task()
        if task is None:
            raise HomeAssistantError('Unable to track the washer transaction')
        active = _ACTIVE_TRANSACTIONS.setdefault(entry_id, set())
        active.add(task)
        try:
            async with lock:
                async with device.miot_client.device_operation(device.did):
                    await WasherTransaction(hass, device).async_start(
                        call.data.get(ATTR_DELAY_UNTIL_ENTITY_ID))
        finally:
            active.discard(task)
            if not active:
                _ACTIVE_TRANSACTIONS.pop(entry_id, None)

    hass.services.async_register(
        DOMAIN,
        SERVICE_START_WASHER,
        async_start_washer,
        schema=START_WASHER_SCHEMA)


async def async_prepare_washer_unload(entry_id: str) -> None:
    """Block and cancel washer work before a Xiaomi client is unloaded."""
    _UNLOADING_ENTRIES.add(entry_id)
    tasks = list(_ACTIVE_TRANSACTIONS.get(entry_id, set()))
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


def washer_entry_ready(entry_id: str) -> None:
    """Allow washer work after a Xiaomi config entry has loaded."""
    _UNLOADING_ENTRIES.discard(entry_id)
