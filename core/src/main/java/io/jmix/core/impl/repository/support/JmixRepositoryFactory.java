/*
 * Copyright 2021 Haulmont.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.jmix.core.impl.repository.support;

import io.jmix.core.*;
import io.jmix.core.impl.repository.query.utils.JmixQueryLookupStrategy;
import io.jmix.core.repository.UnsafeDataRepository;
import org.springframework.context.ApplicationContext;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.lang.Nullable;

import java.util.Optional;

public class JmixRepositoryFactory extends RepositoryFactorySupport {

    private ApplicationContext ctx;

    public JmixRepositoryFactory(ApplicationContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public <T, ID> EntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
        return new JmixEntityInformation<>(domainClass, ctx.getBean(EntityStates.class), ctx.getBean(MetadataTools.class));
    }

    @Override
    protected Object getTargetRepository(RepositoryInformation metadata) {
        Object domainClass = metadata.getDomainType();
        UnconstrainedDataManager repositoryDataManager = getAppropriateDataManager(metadata);

        return getTargetRepositoryViaReflection(metadata, domainClass, repositoryDataManager, ctx.getBean(Metadata.class));
    }

    protected UnconstrainedDataManager getAppropriateDataManager(RepositoryInformation metadata) {
        return metadata.getRepositoryInterface().isAnnotationPresent(UnsafeDataRepository.class)
                ? ctx.getBean(UnconstrainedDataManager.class)
                : ctx.getBean(DataManager.class);
    }

    @Override
    protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
        return JmixDataRepositoryImpl.class;
    }


    @Override
    protected Optional<QueryLookupStrategy> getQueryLookupStrategy(@Nullable QueryLookupStrategy.Key key,
                                                                   QueryMethodEvaluationContextProvider evaluationContextProvider) {
        return Optional.of(new JmixQueryLookupStrategy(ctx.getBean(DataManager.class), ctx.getBean(UnconstrainedDataManager.class), ctx.getBean(Metadata.class)));
    }
}
