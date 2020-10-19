/*
 * Copyright 2020 Haulmont.
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

package io.jmix.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.jmix.core.entity.EntityValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class BaseDataStore implements DataStore {

    protected final Multimap<Class<? extends EventObject>, Consumer<? extends EventObject>> listeners =
            HashMultimap.create();

    private static final Logger log = LoggerFactory.getLogger(BaseDataStore.class);

    @Nullable
    @Override
    public <E> E load(LoadContext<E> context) {
        BeforeEntityLoadEvent<E> beforeLoadEvent = new BeforeEntityLoadEvent<>(context);
        fireEvent(beforeLoadEvent);

        if (beforeLoadEvent.loadPrevented()) {
            return null;
        }

        E entity = null;
        Object transaction = beginLoadTransaction(context);
        try {
            entity = loadOne(context);

            EntityLoadedEvent<E> loadEvent = new EntityLoadedEvent<>(context, entity);
            fireEvent(loadEvent);

            entity = loadEvent.getResultEntity();

            commitLoadTransaction(transaction, context,
                    entity == null ? Collections.emptyList() : Collections.singletonList(entity));
        } catch (Throwable e) {
            rollbackTransaction(transaction);
        }

        AfterEntityLoadEvent<E> afterLoadEvent = new AfterEntityLoadEvent<>(context, entity);
        fireEvent(afterLoadEvent);

        return afterLoadEvent.getResultEntity();
    }

    @Override
    public <E> List<E> loadList(LoadContext<E> context) {
        BeforeEntityLoadEvent<E> beforeLoadEvent = new BeforeEntityLoadEvent<>(context);
        fireEvent(beforeLoadEvent);

        if (beforeLoadEvent.loadPrevented()) {
            return Collections.emptyList();
        }

        List<E> resultList = Collections.emptyList();
        Object transaction = beginLoadTransaction(context);
        try {
            if (context.getIds().isEmpty()) {
                List<E> entities = loadAll(context);

                EntityLoadedEvent<E> loadEvent = new EntityLoadedEvent<>(context, entities);
                fireEvent(loadEvent);

                resultList = loadEvent.getResultEntities();

                if (entities.size() != resultList.size()) {
                    Preconditions.checkNotNull(context.getQuery());
                    if (context.getQuery().getMaxResults() != 0) {
                        resultList = loadListByBatches(context, resultList.size());
                    }
                }
            } else {
                resultList = loadAll(context);

                EntityLoadedEvent<E> loadEvent = new EntityLoadedEvent<>(context, resultList);
                fireEvent(loadEvent);

                resultList = checkAndReorderLoadedEntities(context, loadEvent.getResultEntities());
            }

            commitLoadTransaction(transaction, context, resultList);
        } catch (Throwable e) {
            rollbackTransaction(transaction);
        }

        AfterEntityLoadEvent<E> afterLoadEvent = new AfterEntityLoadEvent<>(context, resultList);
        fireEvent(afterLoadEvent);

        return afterLoadEvent.getResultEntities();
    }

    @Override
    public long getCount(LoadContext<?> context) {
        BeforeEntityCountEvent<?> beforeCountEvent = new BeforeEntityCountEvent<>(context);
        fireEvent(beforeCountEvent);

        if (beforeCountEvent.countPrevented()) {
            return 0;
        }

        long count = 0L;
        Object transaction = beginLoadTransaction(context);
        try {
            if (beforeCountEvent.countByItems()) {
                LoadContext<?> countContext = context.copy();
                if (countContext.getQuery() != null) {
                    countContext.getQuery().setFirstResult(0);
                    countContext.getQuery().setMaxResults(0);
                }

                List<?> entities = loadAll(countContext);

                //noinspection unchecked,rawtypes,rawtypes
                EntityLoadedEvent<?> loadEvent = new EntityLoadedEvent(context, entities);
                fireEvent(loadEvent);

                List<?> resultList = loadEvent.getResultEntities();
                count = resultList.size();
            } else {
                count = countAll(context);
            }

            commitLoadTransaction(transaction, context, Collections.emptyList());
        } catch (Throwable e) {
            rollbackTransaction(transaction);
        }
        return count;
    }

    @Nullable
    protected abstract <E> E loadOne(LoadContext<E> context);

    protected abstract <E> List<E> loadAll(LoadContext<E> context);

    protected abstract long countAll(LoadContext<?> context);

    protected abstract Object beginLoadTransaction(LoadContext<?> context);

    protected abstract <E> void commitLoadTransaction(Object transaction, LoadContext<E> context, List<E> entities);


    protected abstract void rollbackTransaction(Object transaction);

    public void registerEventListener(Class<? extends EventObject> eventType, Consumer<? extends EventObject> eventListener) {
        listeners.put(eventType, eventListener);
    }

    public <T extends EventObject> void fireEvent(T event) {

    }

    protected <E> List<E> loadListByBatches(LoadContext<E> context, int actualSize) {
        assert context.getQuery() != null;

        List<E> entities = new ArrayList<>();

        int requestedFirst = context.getQuery().getFirstResult();
        int requestedMax = context.getQuery().getMaxResults();

        int expectedSize = requestedMax + requestedFirst;
        int factor = actualSize == 0 ? 2 : requestedMax / actualSize * 2;

        int firstResult = 0;
        int maxResults = (requestedFirst + requestedMax) * factor;
        int i = 0;
        while (entities.size() < expectedSize) {
            if (i++ > 100000) {
                log.warn("Loading by batches. Endless loop detected for {}", context);
                break;
            }

            //noinspection unchecked
            LoadContext<E> batchContext = (LoadContext<E>) context.copy();

            assert batchContext.getQuery() != null;
            batchContext.getQuery().setFirstResult(firstResult);
            batchContext.getQuery().setMaxResults(maxResults);

            List<E> list = loadAll(batchContext);
            if (list.size() == 0) {
                break;
            }

            EntityLoadedEvent<E> loadEvent = new EntityLoadedEvent<>(context, list);
            fireEvent(loadEvent);

            entities.addAll(loadEvent.getResultEntities());
            firstResult = firstResult + maxResults;
        }

        // Copy by iteration because subList() returns non-serializable class
        int max = Math.min(requestedFirst + requestedMax, entities.size());
        List<E> resultList = new ArrayList<>(max - requestedFirst);
        int j = 0;
        for (E item : entities) {
            if (j >= max)
                break;
            if (j >= requestedFirst)
                resultList.add(item);
            j++;
        }

        return resultList;
    }

    protected <E> List<E> checkAndReorderLoadedEntities(LoadContext<E> context, List<E> entities) {
        List<E> result = new ArrayList<>(context.getIds().size());
        Map<Object, E> idToEntityMap = entities.stream().collect(Collectors.toMap(EntityValues::getId, Function.identity()));
        for (Object id : context.getIds()) {
            E entity = idToEntityMap.get(id);
            if (entity == null) {
                throw new EntityAccessException(context.getEntityMetaClass(), id);
            }
            result.add(entity);
        }
        return result;
    }

    public static class BeforeEntityLoadEvent<E> extends EventObject {
        private static final long serialVersionUID = -6243582872039288321L;

        public BeforeEntityLoadEvent(LoadContext<E> loadContext) {
            super(loadContext);
        }

        @SuppressWarnings("unchecked")
        public LoadContext<E> getLoadContext() {
            return (LoadContext<E>) getSource();
        }

        public boolean loadPrevented() {
            return false;
        }
    }

    public static class AfterEntityLoadEvent<E> extends EventObject {
        private static final long serialVersionUID = -6243582872039288321L;

        protected final Collection<E> entities;

        public AfterEntityLoadEvent(LoadContext<E> loadContext, Collection<E> entities) {
            super(loadContext);
            this.entities = entities;
        }

        public AfterEntityLoadEvent(LoadContext<E> loadContext, @Nullable E entity) {
            super(loadContext);
            this.entities = entity == null ? Collections.emptyList() : Collections.singleton(entity);
        }

        @SuppressWarnings("unchecked")
        public LoadContext<E> getLoadContext() {
            return (LoadContext<E>) getSource();
        }

        public boolean loadPrevented() {
            return false;
        }

        public E getResultEntity() {
            return null;
        }

        public List<E> getResultEntities() {
            return null;
        }
    }

    public static class EntityLoadedEvent<E> extends EventObject {
        private static final long serialVersionUID = -6243582872039288321L;

        protected final Collection<E> entities;

        public EntityLoadedEvent(LoadContext<E> loadContext, Collection<E> entities) {
            super(loadContext);
            this.entities = entities;
        }

        public EntityLoadedEvent(LoadContext<E> loadContext, @Nullable E entity) {
            super(loadContext);
            this.entities = entity == null ? Collections.emptyList() : Collections.singleton(entity);
        }

        @SuppressWarnings("unchecked")
        public LoadContext<E> getLoadContext() {
            return (LoadContext<E>) getSource();
        }

        public boolean loadPrevented() {
            return false;
        }

        @Nullable
        public E getResultEntity() {
            return null;
        }

        public List<E> getResultEntities() {
            return null;
        }
    }

    public static class BeforeEntityCountEvent<E> extends EventObject {
        private static final long serialVersionUID = -6243582872039288321L;

        public BeforeEntityCountEvent(LoadContext<E> loadContext) {
            super(loadContext);
        }

        @SuppressWarnings("unchecked")
        public LoadContext<E> getLoadContext() {
            return (LoadContext<E>) getSource();
        }

        public boolean countPrevented() {
            return false;
        }

        public boolean countByItems() {
            return false;
        }
    }
}
