from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import delete

from module.strategy.schema import CreateStrategyRequest, UpdateStrategyRequest
from module.strategy import StrategyService
from module.strategy.agents.strategy_gen import StrategyGenOutput
from module.strategy.exception import (
    StrategyCreationError,
    StrategyValidationException,
    StrategyNotFoundException,
    StrategyGenerationError,
)
from module.strategy.model import Strategy
from api.routes.util import create_user
from core.db import get_db_sess_sync, get_db_session


@pytest.fixture
def strategy_service():
    return StrategyService()


@pytest.fixture(scope="module", autouse=True)
def clear_table():
    yield

    with get_db_sess_sync() as db_sess:
        db_sess.execute(delete(Strategy))
        db_sess.commit()


@pytest_asyncio.fixture
async def db_sess():
    async with get_db_session() as db_sess:
        yield db_sess


class TestCreateStrategy:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_create_strategy_generation_error_raises(self, strategy_service):
            mock_db_sess = AsyncMock()

            with patch.object(strategy_service, "_generate_strategy_code") as mock_gen:
                mock_gen.side_effect = StrategyGenerationError("Generation failed")

                request = CreateStrategyRequest(description="test strategy")

                with pytest.raises(StrategyGenerationError):
                    await strategy_service.create(request, uuid4(), mock_db_sess)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_create_strategy_validation_error_raises(self, strategy_service):
            mock_db_sess = AsyncMock()

            with patch.object(strategy_service, "_generate_strategy_code") as mock_gen:
                mock_gen.return_value = StrategyGenOutput(
                    name="Test Strategy",
                    description="Test description",
                    code="class Strategy: pass",
                    error=None,
                )

                with patch.object(
                    strategy_service, "_validate_strategy_code"
                ) as mock_validate:
                    mock_validate.side_effect = StrategyValidationException(
                        ["Invalid code"]
                    )

                    request = CreateStrategyRequest(description="test strategy")

                    with pytest.raises(StrategyValidationException):
                        await strategy_service.create(request, uuid4(), mock_db_sess)

        # TODO: Implement agents
        @pytest.mark.skip
        @pytest.mark.asyncio(loop_scope="session")
        async def test_create_strategy_success(self, strategy_service):
            mock_db_sess = AsyncMock()

            mock_strategy = MagicMock()
            mock_strategy.strategy_id = uuid4()
            mock_strategy.name = "Test Strategy"
            mock_strategy.description = "Test description"
            mock_strategy.prompt = "test prompt"
            mock_strategy.created_at = MagicMock()
            mock_strategy.updated_at = MagicMock()

            mock_db_sess.add = MagicMock()
            mock_db_sess.flush = AsyncMock()
            mock_db_sess.refresh = AsyncMock()

            with patch.object(strategy_service, "_generate_strategy_code") as mock_gen:
                mock_gen.return_value = StrategyGenOutput(
                    name="Test Strategy",
                    description="Test description",
                    code="class Strategy: pass",
                    error=None,
                )

                with patch.object(
                    strategy_service, "_validate_strategy_code", return_value=True
                ):
                    request = CreateStrategyRequest(description="test strategy")

                    result = await strategy_service.create(
                        request, uuid4(), mock_db_sess
                    )

                    mock_db_sess.add.assert_called_once()

        # TODO: Implement agents
        @pytest.mark.skip
        @pytest.mark.asyncio(loop_scope="session")
        async def test_create_strategy_throws_validation_error(self, strategy_service):
            mock_db_sess = AsyncMock()
            mock_db_sess.add = MagicMock()
            mock_db_sess.flush = AsyncMock()
            mock_db_sess.refresh = AsyncMock()

            with pytest.raises(StrategyValidationException):
                request = CreateStrategyRequest(description="...")
                result = await strategy_service.create(request, uuid4(), mock_db_sess)

    class TestIntegrationTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_create_strategy_stores_in_db(self, strategy_service, db_sess):
            user = await create_user("create-strategy-user-1")
            user_id = user.user_id

            with patch.object(strategy_service, "_generate_strategy_code") as mock_gen:
                mock_gen.return_value = StrategyGenOutput(
                    name="Integration Strategy",
                    description="Integration test strategy",
                    code="class Strategy:\n    pass",
                    error=None,
                )

                with patch.object(
                    strategy_service, "_validate_strategy_code", return_value=True
                ):
                    request = CreateStrategyRequest(
                        description="integration test strategy"
                    )

                    result = await strategy_service.create(request, user_id, db_sess)
                    await db_sess.commit()

                    async with get_db_session() as new_db_sess:
                        strategy = await new_db_sess.get(Strategy, result.strategy_id)

                    assert strategy is not None
                    assert strategy.user_id == user_id
                    assert strategy.name == "Integration Strategy"
                    assert strategy.description == "Integration test strategy"
                    assert strategy.code == "class Strategy:\n    pass"


class TestUpdateStrategy:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_update_strategy_not_found_raises(self, strategy_service):
            mock_db_sess = AsyncMock()
            mock_db_sess.scalar.return_value = None

            with patch.object(strategy_service, "get_user_strategy") as mock_get:
                mock_get.side_effect = StrategyNotFoundException()

                request = UpdateStrategyRequest(name="New Name")

                with pytest.raises(StrategyNotFoundException):
                    await strategy_service.update(
                        request, uuid4(), uuid4(), mock_db_sess
                    )

        @pytest.mark.asyncio(loop_scope="session")
        async def test_update_strategy_name_success(self, strategy_service):
            mock_db_sess = AsyncMock()

            mock_strategy = MagicMock()
            mock_strategy.name = "Old Name"

            with patch.object(
                strategy_service, "get_user_strategy", return_value=mock_strategy
            ):
                request = UpdateStrategyRequest(name="New Name")

                result = await strategy_service.update(
                    request, uuid4(), uuid4(), mock_db_sess
                )

                assert result.name == "New Name"

        @pytest.mark.asyncio(loop_scope="session")
        async def test_update_strategy_description_success(self, strategy_service):
            mock_db_sess = AsyncMock()

            mock_strategy = MagicMock()
            mock_strategy.description = "Old Description"

            with patch.object(
                strategy_service, "get_user_strategy", return_value=mock_strategy
            ):
                request = UpdateStrategyRequest(description="New Description")

                result = await strategy_service.update(
                    request, uuid4(), uuid4(), mock_db_sess
                )

                assert result.description == "New Description"


class TestDeleteStrategy:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_delete_strategy_not_found_raises(self, strategy_service):
            mock_db_sess = AsyncMock()

            with patch.object(strategy_service, "get_user_strategy") as mock_get:
                mock_get.side_effect = StrategyNotFoundException()

                with pytest.raises(StrategyNotFoundException):
                    await strategy_service.delete(uuid4(), uuid4(), mock_db_sess)

        @pytest.mark.asyncio(loop_scope="session")
        async def test_delete_strategy_calls_delete(self, strategy_service):
            mock_db_sess = AsyncMock()

            mock_strategy = MagicMock()
            mock_delete = AsyncMock()
            mock_db_sess.delete = mock_delete

            with patch.object(
                strategy_service, "get_user_strategy", return_value=mock_strategy
            ):
                await strategy_service.delete(uuid4(), uuid4(), mock_db_sess)

                mock_db_sess.delete.assert_called_once_with(mock_strategy)

    class TestIntegrationTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_delete_strategy_removes_from_db(self, strategy_service, db_sess):
            user = await create_user("delete-strategy-1")
            user_id = user.user_id

            with patch.object(strategy_service, "_generate_strategy_code") as mock_gen:
                mock_gen.return_value = StrategyGenOutput(
                    name="Delete Me",
                    description="Will be deleted",
                    code="class Strategy: pass",
                    error=None,
                )

                with patch.object(
                    strategy_service, "_validate_strategy_code", return_value=True
                ):
                    request = CreateStrategyRequest(description="delete test")

                    strategy = await strategy_service.create(request, user_id, db_sess)
                    await db_sess.commit()

                    strategy_id = strategy.strategy_id

            async with get_db_session() as new_db_sess:
                await strategy_service.delete(strategy_id, user_id, new_db_sess)
                await new_db_sess.commit()

            async with get_db_session() as new_db_sess:
                deleted = await new_db_sess.get(Strategy, strategy_id)

            assert deleted is None


class TestGenerateStrategy:

    class TestUnitTest:

        @pytest.mark.asyncio(loop_scope="session")
        async def test_generate_strategy_code_with_error_raises(self, strategy_service):
            with patch(
                # "api.routes.strategy.service.strategy_gen_agent.run"
                "module.strategy.service.strategy_gen_agent.run"
            ) as mock_run:
                mock_result = MagicMock()
                mock_result.output = StrategyGenOutput(
                    name=None, description=None, code=None, error="AI Error"
                )
                mock_run.return_value = mock_result

                with pytest.raises(StrategyGenerationError, match="AI Error"):
                    await strategy_service._generate_strategy_code("test prompt")

        @pytest.mark.asyncio(loop_scope="session")
        async def test_generate_strategy_code_success(self, strategy_service):
            with patch("module.strategy.service.strategy_gen_agent.run") as mock_run:
                mock_result = MagicMock()
                mock_result.output = StrategyGenOutput(
                    name="Generated Strategy",
                    description="Generated description",
                    code="class Strategy: pass",
                    error=None,
                )
                mock_run.return_value = mock_result

                with patch.object(
                    strategy_service, "_validate_strategy_code", return_value=True
                ):
                    result = await strategy_service._generate_strategy_code(
                        "test prompt"
                    )

                    assert result.name == "Generated Strategy"
                    assert result.description == "Generated description"
