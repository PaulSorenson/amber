FROM pybuild:latest as pybuild

USER ${user}

COPY --chown=${user} pyproject.toml poetry.lock ./
RUN poetry install --no-dev --no-root

COPY --chown=${user} amberapi_v1/ amberapi_v1/
COPY --chown=${user} aioconveyor/ aioconveyor/
RUN poetry build
RUN pip install dist/*whl

FROM pyrun:latest as pyrun

COPY --chown=${user} --from=pybuild ${venv} ${venv}

CMD ["bash"]
