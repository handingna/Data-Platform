<template>
  <div ref="wrapperRef" class="search-select" @keydown="onKeydown">
    <div class="search-select__field">
      <span class="search-select__icon" aria-hidden="true">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
          <circle cx="11" cy="11" r="7" />
          <path d="m20 20-3.5-3.5" />
        </svg>
      </span>
      <input
        ref="inputRef"
        :value="inputValue"
        class="search-select__input"
        :placeholder="placeholder"
        autocomplete="off"
        spellcheck="false"
        @focus="handleFocus"
        @input="handleInput"
        @blur="handleBlur"
      />
      <button
        v-if="inputValue"
        type="button"
        class="search-select__clear"
        aria-label="清空"
        @mousedown.prevent="clearValue"
      >
        ×
      </button>
    </div>

    <div v-if="isOpen" class="search-select__dropdown" role="listbox">
      <div v-if="isLoading" class="search-select__status">加载中...</div>
      <div v-else-if="!options.length" class="search-select__status">无匹配结果</div>
      <button
        v-for="(option, index) in options"
        :key="optionKey(option, index)"
        type="button"
        class="search-select__option"
        :class="{ 'search-select__option--active': index === activeIndex }"
        @mousedown.prevent="selectOption(option)"
      >
        {{ formatOption(option) }}
      </button>
    </div>
  </div>
</template>

<script setup>
import { onBeforeUnmount, onMounted, ref, watch } from 'vue'

const props = defineProps({
  modelValue: { type: [String, Number], default: '' },
  placeholder: { type: String, default: '' },
  fetchOptions: { type: Function, required: true },
  minChars: { type: Number, default: 0 },
  debounceMs: { type: Number, default: 220 },
  maxItems: { type: Number, default: Number.POSITIVE_INFINITY },
})

const emit = defineEmits(['update:modelValue'])

const wrapperRef = ref(null)
const inputRef = ref(null)
const inputValue = ref(String(props.modelValue ?? ''))
const options = ref([])
const isOpen = ref(false)
const isLoading = ref(false)
const activeIndex = ref(-1)

let blurTimer = null
let debounceTimer = null
let requestSeq = 0
const cache = new Map()

watch(
  () => props.modelValue,
  (value) => {
    const next = String(value ?? '')
    if (next !== inputValue.value) {
      inputValue.value = next
    }
  }
)

function formatOption(option) {
  return String(option ?? '')
}

function optionKey(option, index) {
  return `${formatOption(option)}-${index}`
}

function clearTimers() {
  if (blurTimer) {
    clearTimeout(blurTimer)
    blurTimer = null
  }
  if (debounceTimer) {
    clearTimeout(debounceTimer)
    debounceTimer = null
  }
}

function closeDropdown() {
  isOpen.value = false
  activeIndex.value = -1
}

function openDropdown() {
  isOpen.value = true
}

function normalizeOptions(list) {
  const arr = Array.isArray(list) ? list : []
  return arr.slice(0, Math.max(1, props.maxItems))
}

async function runSearch(keyword) {
  const query = String(keyword ?? '').trim()
  if (query.length < props.minChars) {
    options.value = []
    isLoading.value = false
    closeDropdown()
    return
  }

  const cacheKey = query.toLowerCase()
  openDropdown()
  activeIndex.value = -1

  if (cache.has(cacheKey)) {
    options.value = cache.get(cacheKey)
    isLoading.value = false
    return
  }

  const seq = ++requestSeq
  isLoading.value = true
  try {
    const result = await props.fetchOptions(query)
    if (seq !== requestSeq) return
    const next = normalizeOptions(result)
    cache.set(cacheKey, next)
    options.value = next
    openDropdown()
  } catch {
    if (seq === requestSeq) {
      options.value = []
      openDropdown()
    }
  } finally {
    if (seq === requestSeq) {
      isLoading.value = false
    }
  }
}

function scheduleSearch(keyword) {
  if (debounceTimer) clearTimeout(debounceTimer)
  debounceTimer = setTimeout(() => {
    runSearch(keyword)
  }, Math.max(0, props.debounceMs))
}

function handleFocus() {
  openDropdown()
  scheduleSearch(inputValue.value)
}

function handleInput(event) {
  const next = event.target.value
  inputValue.value = next
  emit('update:modelValue', next)
  openDropdown()
  scheduleSearch(next)
}

function selectOption(option) {
  const next = formatOption(option)
  inputValue.value = next
  emit('update:modelValue', next)
  closeDropdown()
  if (inputRef.value) {
    inputRef.value.focus()
  }
}

function clearValue() {
  inputValue.value = ''
  emit('update:modelValue', '')
  options.value = []
  closeDropdown()
  if (inputRef.value) {
    inputRef.value.focus()
  }
}

function moveActive(step) {
  if (!options.value.length) return
  const len = options.value.length
  activeIndex.value = (activeIndex.value + step + len) % len
}

function onKeydown(event) {
  if (!isOpen.value && (event.key === 'ArrowDown' || event.key === 'ArrowUp')) {
    openDropdown()
    scheduleSearch(inputValue.value)
    event.preventDefault()
    return
  }

  if (event.key === 'ArrowDown') {
    moveActive(1)
    event.preventDefault()
  } else if (event.key === 'ArrowUp') {
    moveActive(-1)
    event.preventDefault()
  } else if (event.key === 'Enter') {
    if (isOpen.value && activeIndex.value >= 0 && activeIndex.value < options.value.length) {
      selectOption(options.value[activeIndex.value])
      event.preventDefault()
    }
  } else if (event.key === 'Escape') {
    closeDropdown()
  }
}

function handleBlur() {
  blurTimer = setTimeout(() => {
    closeDropdown()
  }, 120)
}

function handleDocumentPointerDown(event) {
  const root = wrapperRef.value
  if (!root || root.contains(event.target)) return
  closeDropdown()
}

onMounted(() => {
  document.addEventListener('pointerdown', handleDocumentPointerDown)
})

onBeforeUnmount(() => {
  clearTimers()
  document.removeEventListener('pointerdown', handleDocumentPointerDown)
})
</script>

<style scoped>
.search-select {
  position: relative;
  width: 240px;
  min-width: 240px;
  flex: 0 0 240px;
}

.search-select__field {
  position: relative;
  width: 100%;
}

.search-select__input {
  width: 100%;
  height: 36px;
  padding: 0 36px 0 32px;
  border-radius: 10px;
  border: 1px solid var(--border);
  background: var(--surface-0);
  color: var(--text);
  outline: none;
}

.search-select__input:focus {
  border-color: rgba(79, 124, 255, 0.32);
  box-shadow: 0 0 0 2px rgba(79, 124, 255, 0.08);
}

.search-select__icon,
.search-select__clear {
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
  display: inline-flex;
  align-items: center;
  justify-content: center;
  color: var(--text-soft);
}

.search-select__icon {
  left: 10px;
  width: 14px;
  height: 14px;
  pointer-events: none;
}

.search-select__icon svg {
  width: 14px;
  height: 14px;
}


.search-select__clear {
  right: 8px;
  width: 22px;
  height: 22px;
  border: 0;
  border-radius: 999px;
  background: rgba(148, 163, 184, 0.08);
  color: var(--text);
  cursor: pointer;
  line-height: 1;
}

.search-select__clear:hover {
  background: rgba(79, 124, 255, 0.12);
}

.search-select__dropdown {
  position: absolute;
  left: 0;
  right: 0;
  top: calc(100% + 6px);
  z-index: 20;
  max-height: 320px;
  overflow: auto;
  border: 1px solid var(--border);
  border-radius: 12px;
  background: var(--surface-0);
  box-shadow: var(--shadow-lg);
  backdrop-filter: blur(10px);
}

.search-select__option,
.search-select__status {
  display: block;
  width: 100%;
  padding: 9px 12px;
  border: 0;
  text-align: left;
  background: transparent;
  color: var(--text);
  font: inherit;
}

.search-select__option {
  cursor: pointer;
}

.search-select__option:hover,
.search-select__option--active {
  background: rgba(79, 124, 255, 0.12);
}

.search-select__status {
  color: var(--text-muted);
  cursor: default;
}
</style>



