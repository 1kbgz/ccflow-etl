from typing import List, Optional, Tuple

from ccflow import CallableModel, CallableModelGenericType, Flow

from .backfill import BackfillModel
from .common import ETLContext, ETLResult
from .extract import ExtractModel
from .load import LoadModel
from .transform import TransformModel

__all__ = ("ETL",)


class ETL(CallableModel):
    # Generic Flags
    debug: Optional[bool] = False

    # Sub Models
    extract: Optional[ExtractModel] = None
    transform: Optional[TransformModel] = None
    load: Optional[LoadModel] = None

    # Meta Models
    backfill: Optional[BackfillModel] = None

    @Flow.deps
    def __deps__(self, context: ETLContext) -> List[Tuple[CallableModelGenericType[ETLContext, ETLResult], List[ETLContext]]]:
        deps = []

        if self.extract:
            # add step for top level context
            deps.append((self.extract, [context]))
        if self.transform:
            if self.extract:
                self.transform._extract = self.extract
            # add step for top level context
            deps.append((self.transform, [context]))
        if self.load:
            if self.extract:
                self.load._extract = self.extract
            if self.transform:
                self.load._transform = self.transform
            # add step for top level context
            deps.append((self.load, [context]))
        return deps

    @Flow.call
    def __call__(self, context: ETLContext) -> ETLResult:
        extract_results = []
        transform_results = []
        load_results = []

        if self.extract:
            extract_results = [self.extract(extract_step) for extract_step in context.extract] + [self.extract(context)]
        if self.transform:
            transform_results = [self.transform(transform_step) for transform_step in context.transform] + [self.transform(context)]
        if self.load:
            load_results = [self.load(load_step) for load_step in context.load] + [self.load(context)]
        return ETLResult(step="top", status="success", extract=extract_results, transform=transform_results, load=load_results)
