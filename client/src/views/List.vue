<template>
	<div class="virtual-scroll" ref="scrollRef" @scroll="onScroll" :style="{ maxHeight: maxHeightPx }">
		<table class="virtual-table">
			<thead>
				<tr>
					<th v-for="(header, idx) in leadingHeaders" :key="`lead-h-${idx}`">{{ header }}</th>
					<th v-for="(header, idx) in headers" :key="`h-${idx}`">{{ header }}</th>
				</tr>
			</thead>
			<tbody>
				<tr v-if="rows.length === 0">
					<td :colspan="totalColumns" class="empty">No data</td>
				</tr>
				<template v-else>
					<tr v-if="paddingTop > 0" class="spacer" :style="{ height: `${paddingTop}px` }">
						<td :colspan="totalColumns"></td>
					</tr>
					<tr
						v-for="(row, localIndex) in visibleRows"
						:key="renderStart + localIndex"
						:style="{ height: `${rowHeight}px` }"
					>
						<template v-for="(_, leadIdx) in leadingHeaders" :key="`lead-c-${leadIdx}`">
							<td>
								<slot
									name="leading-cell"
									:row="row"
									:row-index="renderStart + localIndex"
									:leading-index="leadIdx"
								>
									{{ renderStart + localIndex + 1 }}
								</slot>
							</td>
						</template>
						<td v-for="(value, colIndex) in row" :key="`c-${colIndex}`">
							<slot
								name="cell"
								:value="value"
								:row="row"
								:row-index="renderStart + localIndex"
								:col-index="colIndex"
								:header="headers[colIndex]"
							>
								{{ value }}
							</slot>
						</td>
					</tr>
					<tr v-if="paddingBottom > 0" class="spacer" :style="{ height: `${paddingBottom}px` }">
						<td :colspan="totalColumns"></td>
					</tr>
				</template>
			</tbody>
		</table>
	</div>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'

const props = defineProps({
	headers: { type: Array, default: () => [] },
	rows: { type: Array, default: () => [] },
	rowHeight: { type: Number, default: 48 },
	visibleCount: { type: Number, default: 12 },
	leadingHeaders: { type: Array, default: () => [] }
})

const scrollRef = ref(null)
const startIndex = ref(0)
const maxHeightPx = computed(() => `${props.visibleCount * props.rowHeight}px`)
const totalColumns = computed(() => props.headers.length + props.leadingHeaders.length)
const buffer = 4
const safeStart = computed(() => Math.max(startIndex.value - buffer, 0))
const endIndex = computed(() => Math.min(startIndex.value + props.visibleCount + buffer, props.rows.length))
const renderStart = computed(() => safeStart.value)
const visibleRows = computed(() => props.rows.slice(safeStart.value, endIndex.value))
const paddingTop = computed(() => safeStart.value * props.rowHeight)
const paddingBottom = computed(() => Math.max(props.rows.length - endIndex.value, 0) * props.rowHeight)

const onScroll = () => {
	const el = scrollRef.value
	if (!el) return
	startIndex.value = Math.floor(el.scrollTop / props.rowHeight)
}

watch(
	() => props.rows.length,
	() => {
		const el = scrollRef.value
		if (el && startIndex.value > props.rows.length) {
			startIndex.value = 0
			el.scrollTop = 0
		}
	}
)

let resizeObserver = null
onMounted(() => {
	const el = scrollRef.value
	if (!el || typeof ResizeObserver === 'undefined') return
	resizeObserver = new ResizeObserver(() => {
		startIndex.value = Math.floor((el.scrollTop || 0) / props.rowHeight)
	})
	resizeObserver.observe(el)
})

onBeforeUnmount(() => {
	if (resizeObserver && scrollRef.value) {
		resizeObserver.unobserve(scrollRef.value)
	}
})
</script>

<style scoped>
.virtual-scroll {
	width: 100%;
	overflow-y: auto;
	overflow-x: auto;
}

.virtual-table {
	width: 100%;
	border-collapse: collapse;
}

.virtual-table .spacer td {
	padding: 0;
	border: none;
}

.virtual-table .empty {
	text-align: center;
	color: #95a5a6;
}
</style>
