import asyncio
import uuid
import pytest
from uuid import UUID
from unittest.mock import patch, AsyncMock, MagicMock

import pytest_asyncio

from service.deployment.railway.service import RailwayDeploymentService

BACKTEST_ID = UUID("11111111-1111-1111-1111-111111111111")
DEPLOYMENT_ID = UUID("22222222-2222-2222-2222-222222222222")
SERVICE_ID = "test-service-id-123"
SERVICE_IDS = []


@pytest.fixture(autouse=True)
@pytest.skip("Infrastructure not available", allow_module_level=True)
def skip_tests():
    pass


class MockResponse:
    def __init__(self, data, status=200):
        self._data = data
        self.status = status

    async def json(self):
        return self._data

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass


@pytest_asyncio.fixture(scope="module", autouse=True)
async def delete_all_services():
    yield
    service = RailwayDeploymentService()
    await service.init()
    for service_id in SERVICE_IDS:
        await service.stop_service(service_id)


@pytest.fixture
def service():
    return RailwayDeploymentService()


@pytest.fixture
def mock_service():
    with patch("service.deployment.railway.service.ClientSession") as MockClientSession:
        mock_session = MagicMock()
        MockClientSession.return_value = mock_session

        service = RailwayDeploymentService()
        service._http_sess = mock_session
        return service


class TestDeployBacktest:

    @pytest.mark.asyncio
    async def test_deploy_backtest_creates_service(self, service):
        await service.init()

        service.set_backtest_service_id = AsyncMock()

        backtest_id = uuid.uuid4()
        result = await service.deploy_backtest(backtest_id)
        SERVICE_IDS.append(result["service_id"])

        await asyncio.sleep(3)
        get_result = await service.get_service_by_id(result["service_id"])
        assert get_result["id"] == result["service_id"]

    @pytest.mark.asyncio
    async def test_deploy_backtest_stores_service_id_in_db(self, service):
        await service.init()
        backtest_id = uuid.uuid4()

        mock = AsyncMock()
        service.set_backtest_service_id = mock

        result = await service.deploy_backtest(backtest_id)
        SERVICE_IDS.append(result["service_id"])

        assert mock.call_count == 1
        args, kwargs = mock.call_args
        assert args[0] == backtest_id
        assert args[1] == result["service_id"]


class TestStopBacktest:
    @pytest.mark.asyncio
    async def test_stop_backtest_deletes_service(self, service):
        await service.init()
        result = await service.deploy_backtest(uuid.uuid4())
        SERVICE_IDS.append(result["service_id"])

        await asyncio.sleep(5)

        stop_result = await service.stop_backtest(result["service_id"])
        SERVICE_IDS.append(stop_result["service_id"])

        await asyncio.sleep(5)
        get_result = await service.get_service_by_id(result["service_id"])
        assert get_result is None


class TestDeployStrategy:
    @pytest.mark.asyncio
    async def test_deploy_strategy_creates_service(self, service):
        await service.init()
        result = await service.deploy_strategy(DEPLOYMENT_ID)
        SERVICE_IDS.append(result["service_id"])

        await asyncio.sleep(5)
        get_result = await service.get_service_by_id(result["service_id"])
        assert get_result["id"] == result["service_id"]
        assert result["service_name"] == f"dp_{DEPLOYMENT_ID}"


class TestStopStrategy:
    @pytest.mark.asyncio
    async def test_stop_strategy_deletes_service(self, service):
        await service.init()
        result = await service.deploy_strategy(uuid.uuid4())
        SERVICE_IDS.append(result["service_id"])

        await asyncio.sleep(5)

        stop_result = await service.stop_strategy(result["service_id"])
        SERVICE_IDS.append(stop_result["service_id"])

        await asyncio.sleep(5)
        get_result = await service.get_service_by_id(result["service_id"])
        assert get_result is None


class TestStopAll:
    @pytest.mark.asyncio
    async def test_stop_all_removes_all_services(self, service):
        await service.init()

        bts = []
        for i in range(2):
            result = await service.deploy_backtest(uuid.uuid4())
            bts.append(result["service_id"])
            SERVICE_IDS.append(result["service_id"])

        dps = []
        for i in range(2):
            result = await service.deploy_strategy(uuid.uuid4())
            dps.append(result["service_id"])
            SERVICE_IDS.append(result["service_id"])

        await asyncio.sleep(10)

        stop_result = await service.stop_all()

        await asyncio.sleep(10)
        assert stop_result["stopped"] == 4

    @pytest.mark.asyncio
    async def test_stop_all_returns_empty_when_no_services(self, service):
        await service.init()
        result = await service.stop_all()

        assert result["stopped"] == 0
        assert result["message"] == "No running services found"


class TestServiceCreation:
    @pytest.mark.asyncio
    async def test_create_service_calls_graphql(self, service):
        await service.init()
        result = await service._create_service("test-service")
        SERVICE_IDS.append(result)

        await asyncio.sleep(5)
        get_result = await service.get_service_by_id(result)
        assert get_result["id"] == result

    @pytest.mark.asyncio
    async def test_update_service_calls_graphql(self, service):
        await service.init()
        result = await service._create_service("test-update-service")
        SERVICE_IDS.append(result)

        await asyncio.sleep(5)

        await service.update_service(result, "pytest .")

        await asyncio.sleep(5)
        get_result = await service.get_service_by_id(result)
        print(get_result)

    @pytest.mark.asyncio
    async def test_deploy_service_calls_graphql(self, service):
        await service.init()
        result = await service._create_service("test-deploy-service")
        SERVICE_IDS.append(result)

        await asyncio.sleep(5)

        deploy_result = await service.deploy_service(result)
        print(deploy_result)

        await asyncio.sleep(5)
        get_result = await service.get_service_by_id(result)
        print(get_result)

    @pytest.mark.asyncio
    async def test_stop_service_calls_graphql(self, service):
        await service.init()
        result = await service._create_service("test-stop-service")
        SERVICE_IDS.append(result)

        await asyncio.sleep(5)

        await service.stop_service(result)

        await asyncio.sleep(5)
        get_result = await service.get_service_by_id(result)
        assert get_result is None
